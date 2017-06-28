<?php

/**
 * Shared configuration service
 */
$di->setShared('config', function () {
    return new \Phalcon\Config([
        'cassandra' => [
            'adapter' => 'Cassandra',
            'hosts' => [ '172.30.100.104'],
            'keyspace' => 'testphalcon',
            'consistency' => 'ONE', // Infos here \cassandra\ConsistencyLevel
            'retryPolicies' => 'DefaultPolicy',
        ]
    ]);
});

/**
 * This service returns a Cassandra database
 */
$di->setShared('dbCassandra', function () {
    $config = $this->getConfig();
    $connection = new \Phalcon\Db\Adapter\Cassandra($config->cassandra->toArray());
    return $connection;
});

/**
 * If the configuration specify the use of metadata adapter use it or use memory otherwise
 */
$di->setShared('modelsManager', function () {
    $eventsManager = new Phalcon\Events\Manager();
    $modelsManager = new Phalcon\Mvc\Model\Manager();
    $modelsManager->setEventsManager($eventsManager);

    //Attach a listener to models-manager
    $eventsManager->attach('modelsManager', new Phalcon\Annotations\ModelListener());
    return $modelsManager;
});


/**
 * If the configuration specify the use of metadata adapter use it or use memory otherwise
 */
$di->setShared('modelsMetadata', function () {

    $metaData = new Phalcon\Mvc\Model\Metadata\Memory();

    //Set a custom meta-data database introspection
    $metaData->setStrategy(new Phalcon\Annotations\ModelStrategy());
    return $metaData;
});

$di->setShared('annotations', function () {
    return new Phalcon\Annotations\Adapter\Memory();
});

