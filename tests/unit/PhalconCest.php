<?php

require dirname(__DIR__) . '/_bootstrap.php';


class PhalconCest
{
    public function version(\UnitTester $I)
    {
        $I->assertEquals(\Phalcon\Version::get(), '3.2.0');
    }
}
























