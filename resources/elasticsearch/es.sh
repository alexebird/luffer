#!/bin/bash

check_required_env() {
  if [ -z "${ES_HOST}" ]; then
	echo 'must export ES_HOST'
  fi
}

put_plays_template() {
  local templ='plays.exp'
  curl "${ES_HOST}/_template/${templ}" -XPUT -d@"templates/${templ}.json" | jq '.'
}

put_cluster_settings() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
    "persistent" : {
        "indices.store.throttle.max_bytes_per_sec" : "200mb"
    },
    "transient" : {
	"indices.store.throttle.type" : "none"
    }
}
HERE
}

disable_cluster_settings_for_fast_index() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
    "transient" : {
	"indices.store.throttle.type" : "merge"
    }
}
HERE
}

enable_cluster_settings_for_fast_index() {
  curl "${ES_HOST}/_cluster/settings" -XPUT -d@- <<-HERE | jq '.'
{
    "transient" : {
	"indices.store.throttle.type" : "none"
    }
}
HERE
}

main() {
  check_required_env
  eval "${1}"
}

main "${1}"
