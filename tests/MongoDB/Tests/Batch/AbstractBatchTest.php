<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;
use MongoDB\Tests\BaseFunctionalTestCase;
use MongoWriteConcernException;

abstract class AbstractBatchFunctionalTest extends BaseFunctionalTestCase
{
    protected function assertDoesNotHaveWriteConcernError(array $result)
    {
        $constraint = $this->logicalOr(
            $this->logicalNot($this->arrayHasKey('writeConcernError')),
            $this->callback(function($array) {
                return empty($array['writeConcernError']);
            })
        );

        $this->assertThat($result, $constraint);
    }

    protected function assertHasWriteConcernError(array $result)
    {
        $this->assertArrayHasKey('writeConcernError', $result);
        $this->assertInternalType('array', $result['writeConcernError']);
        $this->assertNotEmpty($result['writeConcernError']);
    }

    protected function assertNumWriteErrors($numWriteErrors, array $result)
    {
        $constraint = $this->logicalAnd(
            $this->arrayHasKey('writeErrors'),
            $this->callback(function($result) use ($numWriteErrors) {
                return is_array($result['writeErrors']) && $numWriteErrors === count($result['writeErrors']);
            })
        );

        if (0 === $numWriteErrors) {
            $constraint = $this->logicalOr($constraint, $this->logicalNot($this->arrayHasKey('writeErrors')));
        }

        $this->assertThat($result, $constraint);
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
}
