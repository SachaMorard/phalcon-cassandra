<?php
namespace Codeception\Module;

// here you can define custom actions
// all public methods declared in helper class will be available in $I

use Phalcon\Di;

class UnitHelper extends \Codeception\Module
{
    private $_dependencyInjector;

    public function getDi()
    {
        if(!is_object($this->_dependencyInjector)){
            $this->_dependencyInjector = Di::getDefault();
        }
        return $this->_dependencyInjector;
    }

    public function setDi(\Phalcon\DiInterface $dependencyInjector)
    {
        $this->_dependencyInjector = $dependencyInjector;
    }

    public function getCassandra()
    {
        $di = $this->getDi();
        $cassa = $di->get('dbCassandra');
        return $cassa;
    }

    public function getConfig()
    {
        return $this->getDi()->get('config');
    }
}