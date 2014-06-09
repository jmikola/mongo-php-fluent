<?php

namespace MongoDB\Batch;

use MongoClient;
use MongoCollection;
use MongoDB;

final class OrderedGenerator extends AbstractGenerator
{
    private $operationsIndex = 0;

    /**
     * Constructor.
     *
     * @param MongoClient     $client       MongoClient instance
     * @param MongoDB         $db           MongoDB instance
     * @param MongoCollection $collection   MongoCollection instance
     * @param array           $operations   Operation type/document tuples
     * @param array           $writeConcern Write concern
     */
    public function __construct(MongoClient $client, MongoDB $db, MongoCollection $collection, array $operations, array $writeConcern = array())
    {
        $writeOptions = array('ordered' => true) + $writeConcern;

        parent::__construct($client, $db, $collection, $operations, $writeOptions);
    }

    /**
     * Rewind to the first batch.
     *
     * @see AbstractGenerator::rewind()
     */
    public function rewind()
    {
        $this->operationsIndex = 0;

        return parent::rewind();
    }

    /**
     * Initialize the current batch.
     *
     * Batches will be generated so that operations will execute in order. Since
     * batches are specific to an operation type, this means that a new batch
     * must be created whenever an operation's type differs from that of the
     * previous operation.
     */
    protected function initCurrentBatch()
    {
        if ($this->currentBatch !== null) {
            return;
        }

        while ($this->operationsIndex < count($this->operations)) {
            list($type, $document) = $this->operations[$this->operationsIndex];

            if ($this->currentBatch === null) {
                $this->currentBatch = new MappedBatch($this->createBatchForType($type));
            }

            if ($type !== $this->currentBatch->getType()) {
                return;
            }

            $this->currentBatch->add($document, $this->operationsIndex);
            $this->operationsIndex += 1;
        }
    }
}
