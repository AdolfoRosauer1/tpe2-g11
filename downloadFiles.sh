#!/bin/bash

read -p "Enter your Pampero username: " username

if [ -z "$username" ]; then
  echo "Username cannot be empty. Please run the script again and provide a valid username."
  exit 1
fi

scp "$username"@pampero.itba.edu.ar:/afs/it.itba.edu.ar/pub/pod/ticketsNYC.csv .
scp "$username"@pampero.itba.edu.ar:/afs/it.itba.edu.ar/pub/pod/infractionsNYC.csv .
scp "$username"@pampero.itba.edu.ar:/afs/it.itba.edu.ar/pub/pod/ticketsCHI.csv .
scp "$username"@pampero.itba.edu.ar:/afs/it.itba.edu.ar/pub/pod/infractionsCHI.csv .

echo "Files have been successfully downloaded to the current directory."
