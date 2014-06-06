<?php

namespace MongoDB\Tests\Bulk;

use MongoDB\Bulk\UnorderedBulk;

class UnorderedBulkTest extends AbstractBulkTest
{
    public function testIsOrdered()
    {
        $bulk = $this->getBulk();
        $this->assertFalse($bulk->isOrdered());
    }

    /**
     * @see AbstractBulk::getBulk()
     */
    protected function getBulk()
    {
        return new UnorderedBulk($this->getMockMongoClient(), $this->getDatabase(), $this->getCollection());
    }
}
