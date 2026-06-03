#!/usr/bin/env bash
set -e

: "${SSD_USER:?SSD_USER is required}"
: "${SSD_UID:?SSD_UID is required}"
: "${SSD_GID:?SSD_GID is required}"

SSD_HOME="/home/${SSD_USER}"

if ! getent group "${SSD_GID}" >/dev/null 2>&1; then
  groupadd -g "${SSD_GID}" "${SSD_USER}"
fi

if ! getent passwd "${SSD_UID}" >/dev/null 2>&1; then
  useradd \
    -u "${SSD_UID}" \
    -g "${SSD_GID}" \
    -d "${SSD_HOME}" \
    -s /bin/bash \
    "${SSD_USER}"
fi

passwd -d "${SSD_USER}" 2>/dev/null || true
passwd -u "${SSD_USER}" 2>/dev/null || true

mkdir -p "${SSD_HOME}"
mkdir -p "${SSD_HOME}/.ansible/tmp"
mkdir -p /app/data /app/logs

chown "${SSD_UID}:${SSD_GID}" "${SSD_HOME}" || true
chown -R "${SSD_UID}:${SSD_GID}" "${SSD_HOME}/.ansible" || true
chown -R "${SSD_UID}:${SSD_GID}" /app/data /app/logs || true

chmod 700 "${SSD_HOME}/.ansible/tmp" || true

echo "${SSD_USER} ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/${SSD_USER}"
chmod 0440 "/etc/sudoers.d/${SSD_USER}"

export USER="${SSD_USER}"
export HOME="${SSD_HOME}"

exec gosu "${SSD_USER}" "$@"
