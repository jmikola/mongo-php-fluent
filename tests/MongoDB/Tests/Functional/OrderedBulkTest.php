<?php

namespace MongoDB\Tests;

use MongoDB\OrderedBulk;
use MongoClient;

class OrderedBulkTest extends \PHPUnit_Framework_TestCase
{
    public function testFoo()
    {
        $m = new MongoClient;
        $c = $m->test->foo;
        $c->drop();

        $bulk = new OrderedBulk($c);

        $bulk->insert(array('_id' => 1));
        $bulk->insert(array('_id' => 2));
        $bulk->insert(array('_id' => 3));
        $bulk->find(array('_id' => 1))->update(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 1))->updateOne(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 1))->upsert()->update(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 4))->upsert()->updateOne(array('$inc' => array('a' => 1)));
        $bulk->insert(array('_id' => 5));
        $bulk->find(array('_id' => 2))->remove();
        $bulk->find(array('_id' => 3))->removeOne();
        $result = $bulk->execute();

        $this->assertEquals(4, $result['nInserted']);
        $this->assertEquals(3, $result['nUpdated']);
        $this->assertEquals(1, $result['nUpserted']);
        $this->assertEquals(2, $result['nRemoved']);

        //echo json_encode($result, \JSON_PRETTY_PRINT);
    }
}
