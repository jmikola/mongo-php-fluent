<?php

namespace MongoDB\Tests\Bulk;

use MongoDB\Tests\BaseTestCase;

abstract class AbstractBulkTest extends BaseTestCase
{
    /**
     * @expectedException BadMethodCallException
     * @expectedExceptionMessage Cannot call execute() multiple times
     */
    public function testExecuteCannotBeInvokedMultipleTimes()
    {
        $bulk = $this->getBulk();
        $bulk->execute();
        $bulk->execute();
    }

    /**
     * @expectedException PHPUnit_Framework_Error_Warning
     * @expectedExceptionMessage Missing argument
     * @see https://jira.mongodb.org/browse/PHP-982
     */
    public function testFindRequiresArgument()
    {
        $bulk = $this->getBulk();
        $bulk->find();
    }

    /**
     * @expectedException MongoDB\Exception\UnexpectedTypeException
     */
    public function testFindRequiresArgumentToBeArrayOrObject()
    {
        $bulk = $this->getBulk();
        $bulk->find(null);
    }

    /**
     * @expectedException MongoDB\Exception\UnexpectedTypeException
     */
    public function testInsertRequiresArgumentToBeArrayOrObject()
    {
        $bulk = $this->getBulk();
        $bulk->insert(null);
    }

    /**
     * @expectedException BadMethodCallException
     */
    public function testRemoveRequiresSelector()
    {
        $bulk = $this->getBulk();
        $bulk->remove(array());
    }

    /**
     * @expectedException BadMethodCallException
     */
    public function testRemoveOneRequiresSelector()
    {
        $bulk = $this->getBulk();
        $bulk->removeOne(array());
    }

    /**
     * @expectedException BadMethodCallException
     */
    public function testUpdateRequiresSelector()
    {
        $bulk = $this->getBulk();
        $bulk->update(array());
    }

    /**
     * @expectedException MongoDB\Exception\UnexpectedTypeException
     */
    public function testUpdateRequiresArgumentToBeArrayOrObject()
    {
        $bulk = $this->getBulk();
        $bulk->find(array())->update(null);
    }

    /**
     * @expectedException BadMethodCallException
     */
    public function testUpdateOneRequiresSelector()
    {
        $bulk = $this->getBulk();
        $bulk->updateOne(array());
    }

    /**
     * @expectedException MongoDB\Exception\UnexpectedTypeException
     */
    public function testUpdateOneRequiresArgumentToBeArrayOrObject()
    {
        $bulk = $this->getBulk();
        $bulk->find(array())->updateOne(null);
    }

    abstract protected function getBulk();
}
