#include "s3_functions.hpp"
#include "aws_client.hpp"
#include "aws_http_client.hpp"
#include "utils/utils.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListBucketsRequest.h>
#include <aws/s3/model/ListBucketsResult.h>

namespace duckdb {

namespace {

string FromAws(const Aws::String &value) {
	return {value.data(), value.size()};
}

struct S3BucketsBindData : public TableFunctionData {
	string region;
	string secret_name;
};

struct S3BucketsState : public GlobalTableFunctionState {
	unique_ptr<Aws::S3::S3Client> client;
	Aws::S3::Model::ListBucketsRequest request;
	Aws::S3::Model::ListBucketsResult page;
	idx_t cursor = 0;
	bool finished = false;
};

unique_ptr<FunctionData> S3BucketsBind(ClientContext &, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<S3BucketsBindData>();
	for (const auto &parameter : input.named_parameters) {
		auto name = StringUtil::Lower(parameter.first.GetIdentifierName());
		if (parameter.second.IsNull()) {
			throw InvalidInputException("s3_buckets: %s must not be NULL", name);
		}
		auto value = StringValue::Get(parameter.second);
		if (value.empty()) {
			throw InvalidInputException("s3_buckets: %s must not be empty", name);
		}
		if (name == "region") {
			result->region = value;
		} else if (name == "secret") {
			result->secret_name = value;
		}
	}

	names = {"name", "region", "created_at"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> S3BucketsInit(ClientContext &context, TableFunctionInitInput &input) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("s3_buckets: listing S3 buckets is disabled through configuration");
	}
	AwsInstanceBinding aws_binding(DatabaseInstance::GetDatabase(context));
	auto &data = input.bind_data->Cast<S3BucketsBindData>();
	auto entry = FindAwsSecret(context, data.secret_name);
	auto &secret = entry->secret->Cast<KeyValueSecret>();
	auto provider = CredentialsProviderFromSecret(secret, "S3");

	Aws::S3::S3ClientConfiguration config(BuildClientConfigWithCa());
	auto explicit_region = data.region.empty() ? GetSecretString(secret, "region") : data.region;
	auto region = ResolveAwsRegion(context, explicit_region, FromAws(config.profileName));
	if (!region.empty()) {
		config.region = region.c_str();
	}
	config.useUSEast1RegionalEndPointOption = Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::REGIONAL;

	auto result = make_uniq<S3BucketsState>();
	result->client = make_uniq<Aws::S3::S3Client>(provider, nullptr, config);
	// Pagination also makes ListBuckets return BucketRegion, avoiding per-bucket lookups.
	result->request.SetMaxBuckets(1000);
	// A configured endpoint region must not silently restrict an account-wide listing.
	if (!data.region.empty()) {
		result->request.SetBucketRegion(data.region.c_str());
	}
	return std::move(result);
}

void S3BucketsFun(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	AwsInstanceBinding aws_binding(DatabaseInstance::GetDatabase(context));
	auto &state = input.global_state->Cast<S3BucketsState>();

	while (state.cursor == state.page.GetBuckets().size()) {
		if (state.finished) {
			return;
		}
		context.InterruptCheck();
		auto outcome = state.client->ListBuckets(state.request);
		if (!outcome.IsSuccess()) {
			const auto &error = outcome.GetError();
			throw IOException("S3 ListBuckets failed: %s - %s", FromAws(error.GetExceptionName()),
			                  FromAws(error.GetMessage()));
		}
		state.page = outcome.GetResultWithOwnership();
		const auto &token = state.page.GetContinuationToken();
		state.finished = token.empty();
		if (!state.finished && token == state.request.GetContinuationToken()) {
			throw IOException("S3 ListBuckets returned a repeated continuation token");
		}
		state.request.SetContinuationToken(token);
		state.cursor = 0;
	}

	const auto &buckets = state.page.GetBuckets();
	auto count = MinValue<idx_t>(buckets.size() - state.cursor, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		const auto &bucket = buckets[state.cursor + i];
		output.data[0].Append(bucket.NameHasBeenSet() ? Value(FromAws(bucket.GetName())) : Value());
		output.data[1].Append(bucket.BucketRegionHasBeenSet() ? Value(FromAws(bucket.GetBucketRegion())) : Value());
		output.data[2].Append(
		    bucket.CreationDateHasBeenSet()
		        ? Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromEpochMs(bucket.GetCreationDate().Millis())))
		        : Value());
	}
	output.CheckCardinality(count);
	state.cursor += count;
}

} // namespace

void S3Functions::Register(ExtensionLoader &loader) {
	FunctionSignature signature;
	signature.WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("region", LogicalType::VARCHAR).Add("secret", LogicalType::VARCHAR);
	});
	loader.RegisterFunction(
	    TableFunction("s3_buckets", std::move(signature), S3BucketsFun, S3BucketsBind, S3BucketsInit));
}

} // namespace duckdb
