<?php

namespace MongoDB\Bulk;

use MongoDB\Batch\BatchInterface;
use MongoDB\Exception\UnexpectedTypeException;
use BadMethodCallException;
use InvalidArgumentException;
use MongoClient;
use stdClass;

abstract class AbstractBulk implements BulkInterface
{
    private $currentOp;
    private $executed = false;

    protected $client;
    protected $collection;
    protected $db;
    protected $operations = array();

    /**
     * Constructor.
     *
     * @param MongoClient $client
     * @param string      $db
     * @param string      $collection
     */
    public function __construct(MongoClient $client, $db, $collection)
    {
        $this->client = $client;
        $this->db = (string) $db;
        $this->collection = (string) $collection;
    }

    /**
     * Executes all scheduled write operations.
     *
     * @see BulkInterface::execute()
     * @param array $writeConcern
     * @return array
     * @throws BadMethodCallException if the bulk operations have already been executed
     */
    final public function execute(array $writeConcern = array())
    {
        if ($this->executed) {
            throw new BadMethodCallException('Cannot call execute() multiple times');
        }

        // Do not allow an "ordered" option to alter batch execution
        unset($writeConcern['ordered']);

        $bulkResult = new BulkResult();
        $ordered = $this->isOrdered();

        foreach ($this->getMappedBatches() as $batch) {
            $batchResult = $batch->execute($writeConcern);
            $bulkResult->mergeBatchResult($batch, $batchResult);

            /* If an ordered batch has write errors, fail fast and clear any
             * write concern errors (a full-batch write concern cannot be
             * enforced in this case).
             */
            if ($ordered && ! empty($bulkResult->writeErrors)) {
                $bulkResult['writeConcernErrors'] = array();
                break;
            }
        }

        $this->executed = true;

        return $bulkResult;
    }

    /**
     * Sets the query selector for the next update or remove operation.
     *
     * @see BulkInterface::find()
     * @param array|object $query
     * @return self
     * @throws UnexpectedTypeException if $query is neither an array nor an object
     */
    final public function find($query)
    {
        if (is_array($query)) {
            $query = (object) $query;
        }

        if ( ! is_object($query)) {
            throw new UnexpectedTypeException($query, 'array or object');
        }

        $this->currentOp['q'] = $query;

        return $this;
    }

    /**
     * Adds an insert operation for a document.
     *
     * @see BulkInterface::insert()
     * @param array|object $document
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    final public function insert($document)
    {
        if (is_array($document)) {
            $document = (object) $document;
        }

        if ( ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'array or object');
        }

        $this->addOperation(BatchInterface::OP_INSERT, $document);
    }

    /**
     * Return whether bulk operations are ordered.
     *
     * @see BulkInterface::isOrdered()
     * @return boolean
     */
    abstract public function isOrdered();

    /**
     * Adds a remove operation for all documents matching the current selector.
     *
     * @see BulkInterface::remove()
     */
    final public function remove()
    {
        $document = (object) array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'limit' => 0,
        );

        $this->currentOp = array();

        $this->addOperation(BatchInterface::OP_DELETE, $document);
    }

    /**
     * Adds a remove operation for one document matching the current selector.
     *
     * @see BulkInterface::removeOne()
     */
    final public function removeOne()
    {
        $document = (object) array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'limit' => 1,
        );

        $this->currentOp = array();

        $this->addOperation(BatchInterface::OP_DELETE, $document);
    }

    /**
     * Adds an update operation for all documents matching the current selector.
     *
     * If the upsert option is true, only a single document will be updated.
     *
     * @see BulkInterface::update()
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    final public function update($newObj)
    {
        if (is_array($newObj)) {
            $newObj = (object) $newObj;
        }

        if ( ! is_object($newObj)) {
            throw new UnexpectedTypeException($newObj, 'array or object');
        }

        $document = (object) array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'u' => $newObj,
            'multi' => empty($this->currentOp['upsert']),
            'upsert' => ( ! empty($this->currentOp['upsert'])),
        );

        $this->currentOp = array();

        $this->addOperation(BatchInterface::OP_UPDATE, $document);
    }

    /**
     * Adds an update operation for one document matching the current selector.
     *
     * @see BulkInterface::updateOne()
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    final public function updateOne($newObj)
    {
        if (is_array($newObj)) {
            $newObj = (object) $newObj;
        }

        if ( ! is_object($newObj)) {
            throw new UnexpectedTypeException($newObj, 'array or object');
        }

        $document = (object) array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'u' => $newObj,
            'multi' => false,
            'upsert' => ( ! empty($this->currentOp['upsert'])),
        );

        $this->currentOp = array();

        $this->addOperation(BatchInterface::OP_UPDATE, $document);
    }

    /**
     * Set the upsert option for the next update operation.
     *
     * @see BulkInterface::upsert()
     * @param boolean $upsert
     * @return self
     */
    final public function upsert($upsert = true)
    {
        $this->currentOp['upsert'] = (boolean) $upsert;

        return $this;
    }

    /**
     * Get the batches to be executed.
     *
     * @param array $writeConcern
     * @return Iterator of MappedBatch instances
     */
    abstract protected function getMappedBatches(array $writeConcern = array());

    /**
     * Gets the write operations to be executed.
     *
     * @return array of tuples containing each operation's type and document
     */
    final protected function getOperations()
    {
        return $this->operations;
    }

    /**
     * Adds a write operation.
     *
     * @param integer $type
     * @param object  $document
     */
    private function addOperation($type, $document)
    {
        $this->operations[] = array($type, $document);
    }
}