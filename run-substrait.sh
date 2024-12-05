set -eo pipefail

json="$1"
initialResponse="$(curl -X POST localhost:8080/v1/statement/substrait -H 'X-Trino-User:James' -d 'json')"

nextUri="$(echo "$initialResponse" | jq -r '.nextUri')"

while
