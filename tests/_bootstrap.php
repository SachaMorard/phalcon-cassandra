<?php

use Phalcon\Di\FactoryDefault;
use Phalcon\Mvc\Application;


define('BASE_PATH', dirname(__DIR__));
define('APP_NAME', 'test');



/**
 * The FactoryDefault Dependency Injector automatically registers the services that
 * provide a full stack framework. These default services can be overidden with custom ones.
 */
$di = new FactoryDefault();

require BASE_PATH . '/vendor/autoload.php';

/**
 * Include general services
 */
require 'services.php';


/**
 * Include Autoloader
 */
//include BASE_PATH . '/config/loader.php';

/**
 * Handle the request
 */
$application = new Application($di);

echo $application->handle()->getContent();
return;

