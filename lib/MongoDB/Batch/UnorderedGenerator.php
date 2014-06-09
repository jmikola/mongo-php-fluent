<?php

namespace MongoDB\Batch;

use MongoClient;
use MongoCollection;
use MongoDB;

final class UnorderedGenerator extends AbstractGenerator
{
    private $types = array(
        BatchInterface::OP_INSERT,
        BatchInterface::OP_UPDATE,
        BatchInterface::OP_DELETE,
    );

    private $typesIndex = 0;

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
        $writeOptions = array('ordered' => false) + $writeConcern;

        parent::__construct($client, $db, $collection, $operations, $writeOptions);
    }

    /**
     * Rewind to the first batch.
     *
     * @see AbstractGenerator::rewind()
     */
    public function rewind()
    {
        $this->typesIndex = 0;

        return parent::rewind();
    }

    /**
     * Initialize the current batch.
     *
     * Batches will be generated so that all operations of a certain type may
     * execute at once. This means that three batches will be created at most
     * and contain all insert, update, and delete operations, respectively.
     */
    protected function initCurrentBatch()
    {
        if ($this->currentBatch !== null) {
            return;
        }

        while ($this->typesIndex < count($this->types)) {
            $type = $this->types[$this->typesIndex];
            $this->typesIndex += 1;

            $operations = array_filter($this->operations, function($operation) use ($type) {
                // Operation tuple contains [$type, $document]
                return $type === $operation[0];
            });

            if (empty($operations)) {
                continue;
            }

            $this->currentBatch = new MappedBatch($this->createBatchForType($type));
            $this->populateBatch($this->currentBatch, $operations);

            return;
        }
    }

    /**
     * Populate the batch with the operations.
     *
     * @param MappedBatch $batch
     * @param array       $operations
     */
    private function populateBatch(MappedBatch $batch, array $operations)
    {
        foreach ($operations as $bulkIndex => $operation) {
            // Operation tuple contains [$type, $document]
            $batch->add($operation[1], $bulkIndex);
        }
    }
}
