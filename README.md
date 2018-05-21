# Storage Extractor

[![Build Status](https://travis-ci.org/keboola/ex-storage.svg?branch=master)](https://travis-ci.org/keboola/ex-storage)

Extracts tables from source bucket from a source project. You need to provide a Storage
token (which identifies both the project and the bucket) from the source project which 
has `read` access to the source bucket **only**.

# Usage

Configuration:

```
{
	"parameters": {
		"#token": "some-token",
		"url": "https://connection.keboola.com/",
		"tableName": "some-table"
	}
}
```

For incremental loading, you can provide the `changedSince` parameter:

```
{
	"parameters": {
		"#token": "some-token",
		"url": "https://connection.keboola.com/",
		"tableName": "some-table",
		"changedSince": "-1 day"
	}
}
```

`changedSince` can be a timestamp or anything which 
[`strtotime`](http://php.net/manual/en/function.strtotime.php) can understand.

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/ex-storage
cd ex-storage
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer ci
```

The following environment variables have to be set:

- KBC_TEST_URL - URL of the source Storage (e.g. https://connection.keboola.com/)
- KBC_TEST_BUCKET - Bucket in the source project
- KBC_TEST_TOKEN - Token to the source project (with read access to the source bucket)
- KBC_TEST_WRITE_TOKEN - Token to the source project (with write access to the source bucket)

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
