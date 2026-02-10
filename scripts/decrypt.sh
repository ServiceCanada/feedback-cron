#!/bin/sh
export GPG_TTY=$(tty)

ls ./src/main/resources

# Decrypt application properties
gpg --quiet --batch --yes  --passphrase="$APPLICATION_PROPERTIES_PASSPHRASE" --output ./src/main/resources/application.properties --decrypt ./src/main/resources/application.properties.gpg

# Decrypt Google service account JSON key (modern format)
gpg --quiet --batch --yes  --passphrase="$APPLICATION_PROPERTIES_PASSPHRASE" --output ./src/main/resources/service-account.json --decrypt ./src/main/resources/service-account.json.gpg

ls ./src/main/resources