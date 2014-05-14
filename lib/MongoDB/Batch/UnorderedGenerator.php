<?php

namespace MongoDB\Batch;

use MongoClient;

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
     * @param MongoClient $client       MongoClient instance
     * @param string      $db           Database name
     * @param string      $collection   Collection name
     * @param array       $operations   Operation type/document tuples
     * @param array       $writeConcern Write concern
     */
    public function __construct(MongoClient $client, $db, $collection, array $operations, array $writeConcern = array())
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

            if ($this->currentBatch === null) {
                $this->currentBatch = new MappedBatch($this->createBatchForType($type));
            }

            $this->populateBatch($this->currentBatch);
            $this->typesIndex += 1;

            /* Empty batches should never be executed, so unset the current
             * batch and allow the next type's batch to be initialized.
             */
            if ($this->currentBatch->isEmpty()) {
                $this->currentBatch = null;
            }
        }
    }

    /**
     * Populate the batch with all documents of its type.
     *
     * @param MappedBatch $batch
     */
    private function populateBatch(MappedBatch $batch)
    {
        $batchType = $batch->getType();

        foreach ($this->operations as $bulkIndex => $operation) {
            list($type, $document) = $operation;

            if ($type === $batchType) {
                $batch->add($document, $bulkIndex);
            }
        }
    }
}
