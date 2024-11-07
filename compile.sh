#!/bin/bash

# Define variables for artifact group and version
GROUP="g11"
VERSION="1.0-SNAPSHOT"

# Directories
SERVER_DIR="server/target"
CLIENT_DIR="client/target"
TMP_DIR="tmp"

# Clean previous temporary directory if it exists
rm -rf "$TMP_DIR"

# Build the project using Maven
mvn clean install

# Create a new temporary directory
mkdir -p "$TMP_DIR"

# Copy the server and client tar.gz files to the temporary directory
cp "$SERVER_DIR/tpe2-$GROUP-server-$VERSION-bin.tar.gz" "$TMP_DIR/"
cp "$CLIENT_DIR/tpe2-$GROUP-client-$VERSION-bin.tar.gz" "$TMP_DIR/"

# Navigate to the temporary directory
cd "$TMP_DIR" || { echo "Failed to enter directory $TMP_DIR"; exit 1; }

# Process the server tar.gz
tar -xzf "tpe2-$GROUP-server-$VERSION-bin.tar.gz"
chmod +x "tpe2-$GROUP-server-$VERSION/run-server.sh"
# Convert Windows line endings to Unix (if necessary)
sed -i -e 's/\r$//' "tpe2-$GROUP-server-$VERSION/"*.sh
# Remove the original tar.gz file
rm "tpe2-$GROUP-server-$VERSION-bin.tar.gz"

# Process the client tar.gz
tar -xzf "tpe2-$GROUP-client-$VERSION-bin.tar.gz"
chmod +x "tpe2-$GROUP-client-$VERSION/"*.sh
# Convert Windows line endings to Unix (if necessary)
sed -i -e 's/\r$//' "tpe2-$GROUP-client-$VERSION/"*.sh
# Remove the original tar.gz file
rm "tpe2-$GROUP-client-$VERSION-bin.tar.gz"

echo "Compilation and setup completed successfully."
