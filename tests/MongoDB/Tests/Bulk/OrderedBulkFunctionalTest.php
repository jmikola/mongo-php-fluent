<?php

namespace MongoDB\Tests\Bulk;

use MongoDB\Bulk\OrderedBulk;
use MongoDB\Tests\BaseFunctionalTestCase;

class OrderedBulkFunctionalTest extends BaseFunctionalTestCase
{
    public function testCombinedWriteOperations()
    {
        $bulk = $this->getOrderedBulk();

        $bulk->insert(array('_id' => 1));
        $bulk->insert(array('_id' => 2));
        $bulk->insert(array('_id' => 3));
        $bulk->find(array())->update(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 2))->updateOne(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 3))->upsert()->update(array('$inc' => array('a' => 1)));
        $bulk->find(array('_id' => 4))->upsert()->updateOne(array('$inc' => array('a' => 1)));
        $bulk->insert(array('_id' => 5));
        $bulk->find(array('_id' => 1))->remove();
        $result = $bulk->execute();

        $cursor = $this->getMongoCollection()->find();
        $cursor->sort(array('_id' => 1));
        $documents = iterator_to_array($cursor);

        $this->assertArrayNotHasKey(1, $documents, '{_id: 1} was deleted');
        $this->assertSame(array('_id' => 2, 'a' => 2), $documents[2], '{_id: 2} was inserted and incremented twice');
        $this->assertSame(array('_id' => 3, 'a' => 2), $documents[3], '{_id: 3} was inserted and incremented twice');
        $this->assertSame(array('_id' => 4, 'a' => 1), $documents[4], '{_id: 3} was upserted and incremented once');
        $this->assertSame(array('_id' => 5), $documents[5], '{_id: 5} was inserted and never incremented');
    }

    private function getOrderedBulk()
    {
        return new OrderedBulk($this->getMongoClient(), $this->getDatabase(), $this->getCollection());
    }
}
