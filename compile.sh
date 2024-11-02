#!/bin/bash

server_dir="server/target"
client_dir="client/target"
tmp="tmp"


rm -rf "$tmp"
mvn clean install
mkdir -p "$tmp"

cp "$server_dir/tpe2-g8-server-2024.1Q-bin.tar.gz" "$tmp/"
cp "$client_dir/tpe2-g8-client-2024.1Q-bin.tar.gz" "$tmp/"
cd "$tmp"

tar -xzf "tpe2-g8-server-2024.1Q-bin.tar.gz"
chmod +x tpe2-g8-server-2024.1Q/run-server.sh
sed -i -e 's/\r$//' tpe2-g8-server-2024.1Q/*.sh
rm "tpe2-g8-server-2024.1Q-bin.tar.gz"


tar -xzf "tpe2-g8-client-2024.1Q-bin.tar.gz"
chmod +x tpe2-g8-client-2024.1Q/*.sh
sed -i -e 's/\r$//' tpe2-g8-client-2024.1Q/*.sh
rm "tpe2-g8-client-2024.1Q-bin.tar.gz"
