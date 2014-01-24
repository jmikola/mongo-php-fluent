<?php

namespace MongoDB\Tests;

use MongoDB\Batch;
use MongoDB\BulkInterface;

class BatchTest extends \PHPUnit_Framework_TestCase
{
    public function testConstructor()
    {
        $batch = new Batch(BulkInterface::OP_INSERT, 10);
        $this->assertEquals(10, $batch->getBaseIndex());
        $this->assertEquals(BulkInterface::OP_INSERT, $batch->getType());

        $batch = new Batch(BulkInterface::OP_UPDATE, 20);
        $this->assertEquals(20, $batch->getBaseIndex());
        $this->assertEquals(BulkInterface::OP_UPDATE, $batch->getType());

        $batch = new Batch(BulkInterface::OP_REMOVE, 30);
        $this->assertEquals(30, $batch->getBaseIndex());
        $this->assertEquals(BulkInterface::OP_REMOVE, $batch->getType());
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Invalid type: 0
     */
    public function testConstructorShouldValidateType()
    {
        new Batch(0, 0);
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Base index cannot be negative: -1
     */
    public function testConstructorShouldValidateBaseIndex()
    {
        new Batch(BulkInterface::OP_INSERT, -1);
    }

    public function testDefaultValues()
    {
        $batch = new Batch(BulkInterface::OP_INSERT, 0);

        $this->assertEquals(0, $batch->getBaseIndex());
        $this->assertEquals(0, $batch->getBsonSize());
        $this->assertEquals(array(), $batch->getDocuments());
        $this->assertEquals(0, $batch->getSize());
        $this->assertEquals(BulkInterface::OP_INSERT, $batch->getType());
    }

    public function testPush()
    {
        $documents = array(
            array('_id' => 1),
            array('_id' => 2, 'foo' => 'bar'),
        );

        $batch = new Batch(BulkInterface::OP_INSERT, 0);

        $batch->push($documents[0], 18);
        $this->assertEquals(18, $batch->getBsonSize());
        $this->assertEquals(array($documents[0]), $batch->getDocuments());
        $this->assertEquals(1, $batch->getSize());

        $batch->push($documents[1], 31);
        $this->assertEquals(18 + 31, $batch->getBsonSize());
        $this->assertEquals($documents, $batch->getDocuments());
        $this->assertEquals(2, $batch->getSize());
    }

    public function testPushShouldCheckBsonSizeLimit()
    {
        $batch = new Batch(BulkInterface::OP_INSERT, 0);
        $batch->push(array('_id' => 1), 16777216);

        $this->assertEquals(16777216, $batch->getBsonSize());
        $this->setExpectedException('OverflowException', 'Batch BSON size cannot exceed');

        // Use an incorrect BSON size to test the exact BSON limit
        $batch->push(array('_id' => 2), 1);
    }

    public function testPushShouldCheckSizeLimit()
    {
        $batch = new Batch(BulkInterface::OP_INSERT, 0);

        for ($i = 0; $i < 1000; $i++) {
            $batch->push(array('_id' => $i), 18);
        }

        $this->assertEquals(1000, $batch->getSize());
        $this->setExpectedException('OverflowException', 'Batch size cannot exceed');

        $batch->push(array('_id' => 1000), 18);
    }
}
