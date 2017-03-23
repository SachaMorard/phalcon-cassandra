# Cassandra library for Phalco

## Requirements

* PHP >= 5.3
* Cassandra php driver https://github.com/datastax/php-driver
* Phalcon >= 3.0 https://github.com/phalcon/cphalcon/

Cassandra php driver (https://github.com/datastax/php-driver) 
hasn't appropriate version number. The only version number 
available is for C/C++ driver version (wich is not the version 
of the extension itself).
Because of it, version of this library follow the cassandra 
php-driver real version.

If you need to use cassandra php-driver v1.2.2, please install 
our library at 1.2.2
Follow the exact same logic for other available versions.

## Installing via Composer

Install composer in a common location or in your project:

```bash
curl -s http://getcomposer.org/installer | php
```

Create the composer.json file as follows:

```json
{
    "require-dev": {
        "sachoo/phalcon-cassandra": "1.3.0"
    }
}
```

Run the composer installer:

```bash
php composer.phar install
```