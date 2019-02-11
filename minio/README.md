# MINIO as S3

Complete guide is available at: https://docs.minio.io

Download server binary:
```
  wget https://dl.minio.io/server/minio/release/linux-amd64/minio
```

Download cli binary:
```
  wget https://dl.minio.io/client/mc/release/linux-amd64/mc
```

Make executable: 
```
  * chmod u+x minio
  * chmod u+x mc
```

Run server:
```
  minio server ./data     # This will create $HOME/.minio config directory
```
or if you want to create config directory at custom location,
```
  minio -C config-minio server ./data
```

Running the server gives the access key and secret key that will be used to connect to the service.

Server configuration for mc:
  > command: mc config host add <ALIAS> <YOUR-S3-ENDPOINT> <ACCESS-KEY> <SECRET-KEY> <API-SIGNATURE> #creates $HOME/.mc directory

Example: ./mc config host add minio http://10.10.10.10 kawkab kawkabsecret S3v4

**Note:** We use *kawkab* as the ACCESS-KEY and *kawkabsecret* as the SECRET-KEY in the code. Moreover, we use *S3v4* as the API signature.

To run a client:
  > ./mc -h
  
**For testing with Kawkab:** Use reset.sh script to run both the zookeeper and minio.

