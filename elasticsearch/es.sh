#!/bin/bash

set -e

check_required_env() {
  if [ -z "${ES_HOST}" ]; then
	echo 'must export ES_HOST'
	exit 1
  fi

  if [ -z "${DO_API_KEY}" ]; then
    echo 'must export DO_API_KEY'
    exit 1
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

tail_ids()
{
  local idx_name="${1:?must pass index}"
  curl -s "${ES_HOST}/${idx_name}/play/_search" -XGET -d@- <<-HERE | jq '.'
{
  "_source": false,
  "size": 3,
  "sort": [{"id": "asc"}],
  "query": { "match_all": {} }
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

rm_alias()
{
  local idx_name="${1:?must pass index}"
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

add_alias()
{
  local idx_name="${1:?must pass index}"
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
  local idx_name="${1:?must pass index}"
  curl -s "${ES_HOST}/${idx_name}" -XPUT | jq '.'
  indices
}

rm_index()
{
  local idx_name="${1:?must pass index}"
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

droplet_ip()
{
  local ip_type="${1:?pass 1:ip_type}"
  local name_prefix="${2:?pass 2:name_prefix}"

  curl -s -XGET -H"Content-Type: application/json" \
    -H"Authorization: Bearer ${DO_API_KEY}" \
    "https://api.digitalocean.com/v2/droplets?page=1&per_page=100" | \
    jq -r ".droplets[] | select((.name | split(\"-20\"))[0] == \"${name_prefix}\") |
    .networks.v4[] | select(.type == \"${ip_type}\") | .ip_address"
}

laptop_public_ip()
{
  curl -s icanhazip.com
}

export_firewall_allows()
{
  local laptop_ip="$(laptop_public_ip)"
  laptop_ip="${laptop_ip:?couldnt get laptop public ip}"
  local exporter_eth1="$(droplet_ip private exporter)"
  exporter_eth1="${exporter_eth1:?couldnt get exporter_eth1}"
  local es_port=9200
  local redis_port=6379
  local pg_port=5432

  cat <<-HERE
ufw allow in on eth0 from ${laptop_ip} to any port ${es_port}
ufw allow in on eth0 from ${laptop_ip} to any port ${redis_port}
ufw allow in on eth1 from ${exporter_eth1} to any port ${es_port}
ufw allow in on eth1 from ${exporter_eth1} to any port ${redis_port}
ufw allow in on eth1 from ${exporter_eth1} to any port ${pg_port}
HERE
}

export_firewall_deletes()
{
  export_firewall_allows | sed -e's/\(ufw\) \(allow\)/\1 delete \2/'
}

main() {
  check_required_env || { exit 1; }
  eval "${@}"
}

main "${@}"
