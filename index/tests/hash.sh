#!/usr/bin/env bash

function md548() {
  local s="$1"
  local md5
  local h
  md5="$(printf "%s" "${s}" | md5sum)"
  h="${md5:4:12}"
  printf '%s %s\n' "${h}" "${s}" >> hash.log
  echo "${h}"
}

function pathHash() {
  local path="$1"
  local dir
  local name
  local dirHash
  dir=$(dirname "${path}")
  name=$(basename "${path}")
  printf '%s %s %s\n' "${path}" "${dir}" "${name}" >> hash.log
  if [[ "${dir}" == '.' ]]; then
    md548 "${name}"
  else
    dirHash=$(pathHash "${dir}")
    md548 "${dirHash}${name}"
  fi
}

if (( $# != 1 )); then
  echo "usage: $(basename "$0") <path>" 1>&2
  exit 1
fi
path="$1"
h=$(pathHash "${path}")
printf '%s %d\n' "${h}" "0x${h}"
