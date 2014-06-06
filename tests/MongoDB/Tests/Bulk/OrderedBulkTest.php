<?php

namespace MongoDB\Tests\Bulk;

use MongoDB\Bulk\OrderedBulk;

class OrderedBulkTest extends AbstractBulkTest
{
    public function testIsOrdered()
    {
        $bulk = $this->getBulk();
        $this->assertTrue($bulk->isOrdered());
    }

    /**
     * @see AbstractBulk::getBulk()
     */
    protected function getBulk()
    {
        return new OrderedBulk($this->getMockMongoClient(), $this->getDatabase(), $this->getCollection());
    }
}
