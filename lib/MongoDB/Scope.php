<?php

namespace MongoDB;

use BadMethodCallException;
use InvalidArgumentException;
use MongoCollection;
use MongoCursor;

class Scope
{
    /**
     * The batch size for a read operation.
     *
     * @var integer
     */
    private $batchSize;

    /**
     * The MongoCollection instance for this Scope.
     *
     * @var MongoCollection
     */
    private $collection;

    /**
     * The $comment cursor option for a read operation.
     *
     * @var string
     */
    private $comment;

    /**
     * The query projection.
     *
     * @var array
     */
    private $fields = array();

    /**
     * The index hint for a read or write operation.
     *
     * @var array|string
     */
    private $hint;

    /**
     * The limit for a read or write operation.
     *
     * @var integer
     */
    private $limit;

    /**
     * The $max cursor option for a read operation.
     *
     * @var array
     */
    private $max;

    /**
     * The $maxScan cursor option for a read operation.
     *
     * @var integer
     */
    private $maxScan;

    /**
     * The $maxTime cursor option for a read operation.
     *
     * @var integer
     */
    private $maxTime;

    /**
     * The $min cursor option for a read operation.
     *
     * @var array
     */
    private $min;

    /**
     * The query selector array.
     *
     * @var array
     */
    private $query = array();

    /**
     * The read preference.
     *
     * @var string
     */
    private $readPreference;

    /**
     * The read preference tags.
     *
     * @var array
     */
    private $readPreferenceTags = array();

    /**
     * The $returnKey cursor option for a read operation.
     *
     * @var boolean
     */
    private $returnKey;

    /**
     * The $showDiskLoc cursor option for a read operation.
     *
     * @var boolean
     */
    private $showDiskLoc;

    /**
     * The skip for a read operation.
     *
     * @var integer
     */
    private $skip;

    /**
     * The $snapshot cursor option for a read operation.
     *
     * @var boolean
     */
    private $snapshot;

    /**
     * The sort order.
     *
     * @var array
     */
    private $sort;

    /**
     * The write concern.
     *
     * @var integer|string
     */
    private $writeConcern;

    /**
     * The $upsert option for a write operation.
     *
     * @var boolean
     */
    private $upsert;

    /**
     * Constructor.
     *
     * @param MongoCollection $collection
     */
    public function __construct(MongoCollection $collection)
    {
        $this->collection = $collection;
    }

    /**
     * Set the $comment cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/comment/
     * @param string $comment
     * @return self
     */
    public function comment($comment)
    {
        $this->comment = $comment;

        return $this;
    }

    /**
     * Return the result count for a read operation.
     *
     * @todo Should this use a MongoCursor and respect limit/skip ($foundOnly)?
     * @return integer
     */
    public function count()
    {
        return $this->createCursor()->count();
    }

    /**
     * Return the query explanation for a read operation.
     *
     * @return array
     */
    public function explain()
    {
        return $this->createCursor()->explain();
    }

    /**
     * Set the query selector.
     *
     * @param array $query
     * @return self
     */
    public function find(array $query)
    {
        $this->query = $query;

        return $this;
    }

    /**
     * Return the result cursor for a read operation.
     *
     * @return MongoCursor
     */
    public function get()
    {
        return $this->createCursor();
    }

    /**
     * Return the first result for a read operation.
     *
     * @return array|null
     */
    public function getOne()
    {
        // @todo Should this respect a skip option?
        return $this->createCursor()->limit(-1)->getNext();
    }

    /**
     * Set the index hint for a read operation.
     *
     * @param array|string $index
     * @return self
     */
    public function hint($index)
    {
        $this->hint = $hint;

        return $this;
    }

    /**
     * Insert a document into the collection.
     *
     * @param array|object $document
     * @param array $options
     * @return array|boolean
     */
    public function insert($document, array $options = array())
    {
        if (isset($this->writeConcern)) {
            $options = array_merge(array('w' => $this->writeConcern), $options);
        }

        return $this->collection->insert($document, $options);
    }

    /**
     * Set the limit.
     *
     * @param integer $limit
     * @return self
     */
    public function limit($limit)
    {
        $this->limit = (integer) $limit;

        return $this;
    }

    /**
     * Set the $max cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/max/
     * @param array $max
     * @return self
     */
    public function max(array $max)
    {
        $this->max = $max;

        return $this;
    }

    /**
     * Set the $maxScan cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/maxScan/
     * @param integer $maxScan
     * @return self
     */
    public function maxScan($maxScan)
    {
        $this->maxScan = (integer) $maxScan;

        return $this;
    }

    /**
     * Set the $maxTime cursor option for a read operation.
     *
     * @param integer $maxTime
     * @return self
     */
    public function maxTime($maxTime)
    {
        $this->maxTime = (integer) $maxTime;

        return $this;
    }

    /**
     * Set the $min cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/min/
     * @param array $min
     * @return self
     */
    public function min(array $min)
    {
        $this->min = $min;

        return $this;
    }

    /**
     * Remove one or more documents from the collection.
     *
     * @param array $options
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     */
    public function remove(array $options = array())
    {
        $defaultOptions = array();

        if ($this->limit > 1) {
            throw new BadMethodCallException('remove() does not support a limit greater than one');
        }

        if ($this->limit === 1) {
            $defaultOptions['justOne'] = true;
        }

        if (isset($this->writeConcern)) {
            $defaultOptions['w'] = $this->writeConcern;
        }

        return $this->collection->remove($this->query, array_merge($defaultOptions, $options));
    }

    /**
     * Replace one or more documents in the collection.
     *
     * @param array|object $newObj
     * @param array $options
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     * @throws InvalidArgumentException if $newObj contains update operators
     */
    public function replace($newObj, array $options = array())
    {
        if ($this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('replace() does not support update operators in $newObj');
        }

        $defaultOptions = array();

        if ($this->limit > 1) {
            throw new BadMethodCallException('replace() does not support a limit greater than one');
        }

        $defaultOptions['multiple'] = ($this->limit !== 1);

        if (isset($this->upsert)) {
            $defaultOptions['upsert'] = $this->upsert;
        }

        if (isset($this->writeConcern)) {
            $defaultOptions['w'] = $this->writeConcern;
        }

        return $this->collection->update($this->query, $newObj, array_merge($defaultOptions, $options));
    }

    /**
     * Set the $returnKey cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/returnKey/
     * @return self
     */
    public function returnKey()
    {
        $this->returnKey = true;

        return $this;
    }

    /**
     * Save a document to the collection.
     *
     * @param array|object $document
     * @param array $options
     * @return array|boolean
     */
    public function save($document, array $options = array())
    {
        if (isset($this->writeConcern)) {
            $options = array_merge(array('w' => $this->writeConcern), $options);
        }

        return $this->collection->save($document, $options);
    }

    /**
     * Set the $showDiskLoc cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/showDiskLoc/
     * @return self
     */
    public function showDiskLoc()
    {
        $this->showDiskLoc = true;

        return $this;
    }

    /**
     * Set the skip for a read operation.
     *
     * @param integer $skip
     * @return self
     */
    public function skip($skip)
    {
        $this->skip = (integer) $skip;

        return $this;
    }

    /**
     * Set the $snapshot cursor option for a read operation.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/snapshot/
     * @return self
     */
    public function snapshot()
    {
        $this->snapshot = true;

        return $this;
    }

    /**
     * Set the sort order.
     *
     * @param array $sort
     * @return self
     */
    public function sort(array $sort)
    {
        $this->sort = $sort;

        return $this;
    }

    /**
     * Update one or more documents in the collection.
     *
     * @param array|object $newObj
     * @param array $options
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     * @throws InvalidArgumentException if $newObj does not contain update operators
     */
    public function update($newObj, array $options = array())
    {
        if ( ! $this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('update() requires update operators in $newObj');
        }

        $defaultOptions = array();

        if ($this->limit > 1) {
            throw new BadMethodCallException('replace() does not support a limit greater than one');
        }

        $defaultOptions['multiple'] = ($this->limit !== 1);

        if (isset($this->upsert)) {
            $defaultOptions['upsert'] = $this->upsert;
        }

        if (isset($this->writeConcern)) {
            $defaultOptions['w'] = $this->writeConcern;
        }

        return $this->collection->update($this->query, $newObj, array_merge($defaultOptions, $options));
    }

    /**
     * Set the batch size for a read operation.
     *
     * @param integer $batchSize
     * @return self
     */
    public function withBatchSize($batchSize)
    {
        $this->batchSize = $batchSize;

        return $this;
    }

    /**
     * Set the read preference.
     *
     * @param string $readPreference
     * @param array $tags
     * @return self
     */
    public function withReadPreference($readPreference, array $tags = array())
    {
        $this->readPreference = $readPreference;
        $this->readPreferenceTags = $tags;

        return $this;
    }

    /**
     * Set the write concern.
     *
     * @param integer|string $writeConcern
     * @return self
     */
    public function withWriteConcern($writeConcern)
    {
        $this->writeConcern = $writeConcern;

        return $this;
    }

    /**
     * Create a MongoCursor based on the Scope's current state.
     *
     * @return MongoCursor
     */
    private function createCursor()
    {
        $cursor = $this->collection->find($this->query, $this->fields);

        $options = array(
            'comment',
            'max',
            'maxScan',
            'maxTime',
            'min',
            'returnKey',
            'showDiskLoc',
            'snapshot',
        );

        foreach ($options as $option) {
            if (isset($this->{$option})) {
                $cursor->addOption('$' . $option, $this->{$option});
            }
        }

        if (isset($this->batchSize)) {
            $cursor->batchSize($this->batchSize);
        }

        if (isset($this->limit)) {
            $cursor->limit($this->limit);
        }

        if (isset($this->skip)) {
            $cursor->skip($this->skip);
        }

        if (isset($this->sort)) {
            $cursor->sort($this->sort);
        }

        if (isset($this->readPreference)) {
            $cursor->setReadPreference($this->readPreference, $this->readPreferenceTags);
        }

        return $cursor;
    }

    /**
     * Checks if $newObj contains an update operator.
     *
     * Only the first key or public property will be checked.
     *
     * @param array|object $newObj
     * @return boolean
     * @throws InvalidArgumentException if $newObj is neither an array nor an object
     */
    private function hasUpdateOperator($newObj)
    {
        if (is_object($newObj)) {
            $newObj = get_object_vars($newObj);
        }

        if ( ! is_array($newObj)) {
            throw new InvalidArgumentException('$newObj must be an array or object');
        }

        if (empty($newObj)) {
            return false;
        }

        reset($newObj);

        return strncmp('$', key($newObj), 1) === 0;
    }
}
