<?php

namespace MongoDB\Tests;

class BaseTestCase extends \PHPUnit_Framework_TestCase
{
    protected function getCollection()
    {
        $class = get_called_class();

        return substr($class, strrpos($class, '\\') + 1);
    }

    protected function getDatabase()
    {
        return isset($_ENV['MONGODB_DATABASE']) ? $_ENV['MONGODB_DATABASE'] : 'test';
    }

    protected function getMockMongoClient()
    {
        return $this->getMockBuilder('MongoClient')
            ->disableOriginalConstructor()
            ->getMock();
    }

    protected function getMockMongoCollection()
    {
        return $this->getMockBuilder('MongoCollection')
            ->disableOriginalConstructor()
            ->getMock();
    }

    protected function getMockMongoDB()
    {
        return $this->getMockBuilder('MongoDB')
            ->disableOriginalConstructor()
            ->getMock();
    }
}
