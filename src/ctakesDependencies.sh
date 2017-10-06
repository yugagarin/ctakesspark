#! /bin/bash

mkdir /mnt/azfiles
sudo mount -t cifs //yvtstorageaccount.file.core.windows.net/ingest /mnt/azfiles -o vers=3.0,username=yvtstorageaccount,password=G4XR1kYMbCFORo812FzWGkAxpHOi/hzyRh3TUfTipLUA7vieUBRBKYZpFGRk2exRKffW1lkh6ET7hVU0wNETLw==,dir_mode=0777,file_mode=0777,sec=ntlmssp
mkdir /tmp/ctakesdependencies/
cp -r /mnt/azfiles/* /tmp/ctakesdependencies/
