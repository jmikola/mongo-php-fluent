<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;
use MongoDB\Tests\BaseFunctionalTestCase;
use MongoWriteConcernException;

abstract class AbstractBatchFunctionalTest extends BaseFunctionalTestCase
{
    protected function assertNumWriteConcernErrors($numWriteConcernErrors, $result)
    {
        $this->assertCountOfOptionalArrayField($numWriteConcernErrors, 'writeConcernErrors', $result);
    }

    protected function assertNumWriteErrors($numWriteErrors, $result)
    {
        $this->assertCountOfOptionalArrayField($numWriteErrors, 'writeErrors', $result);
    }

    protected function executeBatch(BatchInterface $batch)
    {
        try {
            $result = $batch->execute();
        } catch (MongoWriteConcernException $e) {
            $result = $e->getDocument();
        }

        return $result;
    }

    abstract protected function getBatch(array $writeOptions = array());

    private function assertCountOfOptionalArrayField($count, $key, $array)
    {
        $constraint = $this->logicalAnd(
            $this->arrayHasKey($key),
            $this->callback(function($array) use ($count, $key) {
                return is_array($array[$key]) && $count === count($array[$key]);
            })
        );

        if (0 === $count) {
            $constraint = $this->logicalOr($constraint, $this->logicalNot($this->arrayHasKey($key)));
        }

        $this->assertThat($array, $constraint);
    }
}
