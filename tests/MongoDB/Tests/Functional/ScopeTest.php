<?php

namespace MongoDB\Tests\Functional;

use MongoClient;
use MongoDB\Scope;

class ScopeTest extends \PHPUnit_Framework_TestCase
{
    public function testGetOne()
    {
        $mongo = new MongoClient();
        $collection = $mongo->test->foo;
        $collection->insert(array('x' => 1));

        $scope = new Scope($collection);

        $this->assertNotNull($scope->find(array('x' => 1))->getOne());
    }
}
