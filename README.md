okuyama-client-java
===================

Java client library for Okuyama KVS


## Usage

At first, create OkuyamaClientFactory and specify endpoints and pool size.
OkuyamaClientFactory can be held through application life time.

```java
String[] endpoints = {"127.0.0.1:8888", "127.0.0.1:8889"};
int poolSize = 10;

OkuyamaClientFactory factory = new OkuyamaClientFactoryImpl(endpoints, poolSize);
```

When you use client instance, call factory.createClient(). This instance is supposed to retain short time as in function.

```java
OkuyamaClient client = factory.createClient();
```
