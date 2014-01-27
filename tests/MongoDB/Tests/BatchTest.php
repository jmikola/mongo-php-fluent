<?php

namespace MongoDB\Tests;

use MongoDB\Batch;
use MongoDB\BulkInterface;

class BatchTest extends \PHPUnit_Framework_TestCase
{
    public function testConstructor()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $this->assertEquals(BulkInterface::OP_INSERT, $batch->getType());

        $batch = new Batch(BulkInterface::OP_UPDATE);
        $this->assertEquals(BulkInterface::OP_UPDATE, $batch->getType());

        $batch = new Batch(BulkInterface::OP_REMOVE);
        $this->assertEquals(BulkInterface::OP_REMOVE, $batch->getType());
    }

    /**
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage Invalid type: 0
     */
    public function testConstructorShouldNotAllowInvalidType()
    {
        new Batch(0);
    }

    public function testDefaultValues()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);

        $this->assertEquals(0, $batch->getBsonSize());
        $this->assertEquals(array(), $batch->getDocuments());
        $this->assertEquals(0, $batch->getSize());
        $this->assertEquals(BulkInterface::OP_INSERT, $batch->getType());
    }

    public function testAdd()
    {
        $documents = array(
            (object) array('_id' => 1),
            (object) array('_id' => 2, 'foo' => 'bar'),
        );

        $batch = new Batch(BulkInterface::OP_INSERT);

        $batch->add(10, $documents[0], 18);
        $this->assertEquals(18, $batch->getBsonSize());
        $this->assertEquals(10, $batch->getBulkIndex(0));
        $this->assertSame($documents[0], $batch->getDocument(0));
        $this->assertSame(array($documents[0]), $batch->getDocuments());
        $this->assertEquals(1, $batch->getSize());

        $batch->add(20, $documents[1], 31);
        $this->assertEquals(18 + 31, $batch->getBsonSize());
        $this->assertEquals(20, $batch->getBulkIndex(1));
        $this->assertSame($documents[1], $batch->getDocument(1));
        $this->assertSame($documents, $batch->getDocuments());
        $this->assertEquals(2, $batch->getSize());
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage Bulk index cannot be negative: -1
     */
    public function testAddShouldNotAllowNegativeBulkIndex()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $batch->add(-1, (object) array('_id' => 1), 18);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage Document already exists for bulk index: 0
     */
    public function testAddShouldNotAllowDuplicateBulkIndexes()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $batch->add(0, (object) array('_id' => 1), 18);
        $batch->add(0, (object) array('_id' => 2, 'foo' => 'bar'), 31);
    }

    /**
     * @expectedException MongoDB\Exception\UnexpectedTypeException
     * @expectedExceptionMessage Expected argument of type "object", "array" given
     */
    public function testAddShouldRequireObjectDocument()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $batch->add(0, array('_id' => 1), 18);
    }

    public function testAddShouldCheckBsonSizeLimit()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        // Use an contrived BSON sizes to test the exact BSON limit
        $batch->add(0, (object) array('_id' => 1), 16777216);

        $this->assertEquals(16777216, $batch->getBsonSize());
        $this->setExpectedException('OverflowException', 'Batch BSON size cannot exceed 16777216 bytes. Current size is 16777216 bytes. Document is 1 bytes.');

        $batch->add(1, (object) array('_id' => 2), 1);
    }

    public function testAddShouldCheckSizeLimit()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);

        for ($i = 0; $i < 1000; $i++) {
            $batch->add($i, (object) array('_id' => $i), 18);
        }

        $this->assertEquals(1000, $batch->getSize());
        $this->setExpectedException('OverflowException', 'Batch size cannot exceed 1000 documents');

        $batch->add(1000, (object) array('_id' => 1000), 18);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage No document exists for batch index: 0
     */
    public function testGetBulkIndexShouldCheckBounds()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $batch->getBulkIndex(0);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage No document exists for batch index: 0
     */
    public function testGetDocumentShouldCheckBounds()
    {
        $batch = new Batch(BulkInterface::OP_INSERT);
        $batch->getDocument(0);
    }
}
