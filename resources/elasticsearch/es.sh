#!/bin/bash

check_required_env() {
  if [ -z "${ES_HOST}" ]; then
	echo 'must export ES_HOST'
  fi
}

put_plays_template() {
  local templ='plays'
  curl "${ES_HOST}/_template/${templ}" -XPUT -d@"templates/${templ}.json" | jq '.'
}

put_cluster_settings() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
  "persistent": {
    "indices.store.throttle.max_bytes_per_sec": "200mb",
    "indices.store.throttle.type": "merge",
    "bootp.mlockall": true
  }
}
HERE
}

disable_cluster_settings_for_fast_index() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
  "transient": {
    "indices.store.throttle.type": "merge"
  }
}
HERE
}

enable_cluster_settings_for_fast_index() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
  "transient": {
    "indices.store.throttle.type": "none"
  }
}
HERE
}

remove_plays_alias()
{
  local idx_name="${1}"
  local plays_alias_name='plays'
  curl -s "${ES_HOST}/_aliases" -XPOST -d@- <<-HERE | jq '.'
{
  "actions": [
    { "remove": { "index": "${idx_name}", "alias" : "${plays_alias_name}" } }
  ]
}
HERE

  aliases
}

add_plays_alias()
{
  local idx_name="${1}"
  local plays_alias_name='plays'
  curl -s "${ES_HOST}/_aliases" -XPOST -d@- <<-HERE | jq '.'
{
  "actions": [
    { "add": { "index": "${idx_name}", "alias" : "${plays_alias_name}" } }
  ]
}
HERE

  aliases
}

mk_index()
{
  local idx_name="${1}"
  curl -s "${ES_HOST}/${idx_name}" -XPUT | jq '.'
  indices
}

rm_index()
{
  local idx_name="${1}"
  [[ "$2" == 'xXx' ]] || { echo must pass magic ; exit 1 ; }
  curl -s "${ES_HOST}/${idx_name}" -XDELETE | jq '.'
  indices
}

aliases()
{
  curl -s ${ES_HOST}/_cat/aliases?v
}

health()
{
  curl -s ${ES_HOST}/_cat/health?v
}

indices()
{
  curl -s ${ES_HOST}/_cat/indices?v
}

main() {
  check_required_env
  eval "${@}"
}

main "${@}"
