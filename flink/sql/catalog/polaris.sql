CREATE CATALOG polaris WITH (

'type' = 'iceberg',

'catalog-impl' = 
'org.apache.iceberg.rest.RESTCatalog',

'uri' =
'http://polaris:8181/api/catalog',

'warehouse' =
'environment_catalog',

'io-impl' =
'org.apache.iceberg.aws.s3.S3FileIO',

'credential' =
'root:s3cr3t',

'scope' =
'PRINCIPAL_ROLE:ALL',

's3.endpoint' =
'http://minio:9000',

's3.path-style-access' =
'true',

's3.access-key-id' =
'minioadmin',

's3.secret-access-key' =
'minioadmin'

);