<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;
use MongoDB\Tests\BaseFunctionalTestCase;

abstract class AbstractDeleteBatchTest extends BaseFunctionalTestCase
{
    public function testGetItemCount()
    {
        $batch = $this->getBatch();

        $operation = array(
            'q' => array('foo' => 1),
            'limit' => 1,
        );

        $this->assertSame(0, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(1, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(2, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(3, $batch->getItemCount());
    }

    public function testGetType()
    {
        $batch = $this->getBatch();

        $this->assertSame(BatchInterface::OP_DELETE, $batch->getType());
    }

    abstract protected function getBatch(array $writeOptions = array());
}
