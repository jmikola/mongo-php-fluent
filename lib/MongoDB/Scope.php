<?php

namespace MongoDB;

use MongoDB\Exception\ResultException;
use MongoDB\Options\CursorOptions;
use MongoDB\Options\QueryOptions;
use MongoDB\Options\WriteOptions;
use BadMethodCallException;
use InvalidArgumentException;
use MongoCollection;
use UnexpectedValueException;

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
     * The cursor options.
     *
     * @var array
     */
    private $cursorOptions = array();

    /**
     * The query projection.
     *
     * @var array
     */
    private $fields = array();

    /**
     * The limit for a read or write operation.
     *
     * @var integer
     */
    private $limit;

    /**
     * The query selector array.
     *
     * @var array
     */
    private $query = array();

    /**
     * The query options.
     *
     * @var array
     */
    private $queryOptions = array();

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
     * The skip for a read operation.
     *
     * @var integer
     */
    private $skip;

    /**
     * The sort order.
     *
     * @var array
     */
    private $sort;

    /**
     * The write options.
     *
     * @var array
     */
    private $writeOptions = array();

    /**
     * The upsert option for a update operation or findandModify command.
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
     * @return \MongoCursor
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
     * Remove one document matching the current scope and return it.
     *
     * @return array|null
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws ResultException if the findAndModify command fails
     */
    public function getOneAndRemove()
    {
        return $this->findAndModify(array('remove' => true));
    }

    /**
     * Replace one document matching the current scope and return its original
     * value.
     *
     * @param array|object $newObj
     * @return array|null
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj contains update operators
     * @throws ResultException if the findAndModify command fails
     */
    public function getOneAndReplace($newObj)
    {
        if ($this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('getOneAndReplace() does not support update operators in $newObj');
        }

        return $this->findAndModify(array('update' => $newObj, 'new' => false));
    }

    /**
     * Update one document matching the current scope and return its original
     * value.
     *
     * @param array|object $newObj
     * @return array|null
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj does not contain update operators
     * @throws ResultException if the findAndModify command fails
     */
    public function getOneAndUpdate($newObj)
    {
        if ( ! $this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('update() requires update operators in $newObj');
        }

        return $this->findAndModify(array('update' => $newObj, 'new' => false));
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
     * Remove all documents matching the current scope.
     *
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     *                                or the skip offset is greater than zero
     */
    public function remove()
    {
        if ($this->limit > 1) {
            throw new BadMethodCallException('remove() does not support a limit greater than one');
        }

        if ($this->skip > 0) {
            throw new BadMethodCallException('remove() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['justOne'] = ($this->limit === 1);

        return $this->collection->remove($this->query, $options);
    }

    /**
     * Remove one document matching the current scope.
     *
     * @param array $options
     * @return array|boolean
     * @throws BadMethodCallException if the skip offset is greater than zero
     */
    public function removeOne()
    {
        if ($this->skip > 0) {
            throw new BadMethodCallException('removeOne() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['justOne'] = true;

        return $this->collection->remove($this->query, $options);
    }

    /**
     * Replace all documents matching the current scope.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     *                                or the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj contains update operators
     */
    public function replace($newObj)
    {
        if ($this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('replace() does not support update operators in $newObj');
        }

        if ($this->limit > 1) {
            throw new BadMethodCallException('replace() does not support a limit greater than one');
        }

        if ($this->skip > 0) {
            throw new BadMethodCallException('replace() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['multiple'] = ($this->limit !== 1);
        $options['upsert'] = ($this->upsert === true);

        return $this->collection->update($this->query, $newObj, $options);
    }

    /**
     * Replace one document matching the current scope.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj contains update operators
     */
    public function replaceOne($newObj)
    {
        if ($this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('replaceOne() does not support update operators in $newObj');
        }

        if ($this->skip > 0) {
            throw new BadMethodCallException('replaceOne() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['multiple'] = false;
        $options['upsert'] = ($this->upsert === true);

        return $this->collection->update($this->query, $newObj, $options);
    }

    /**
     * Replace one document matching the current scope and return its modified
     * value.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj contains update operators
     * @throws ResultException if the findAndModify command fails
     */
    public function replaceOneAndGet($newObj)
    {
        if ($this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('replaceOneAndGet() does not support update operators in $newObj');
        }

        return $this->findAndModify(array('update' => $newObj, 'new' => true));
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
     * Update all documents matching the current scope.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the limit is set and greater than one
     *                                or the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj does not contain update operators
     */
    public function update($newObj)
    {
        if ( ! $this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('update() requires update operators in $newObj');
        }

        if ($this->limit > 1) {
            throw new BadMethodCallException('update() does not support a limit greater than one');
        }

        if ($this->skip > 0) {
            throw new BadMethodCallException('update() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['multiple'] = ($this->limit !== 1);
        $options['upsert'] = ($this->upsert === true);

        return $this->collection->update($this->query, $newObj, $options);
    }

    /**
     * Update one document matching the current scope.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj contains update operators
     */
    public function updateOne($newObj)
    {
        if ( ! $this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('updateOne() does not support update operators in $newObj');
        }

        if ($this->skip > 0) {
            throw new BadMethodCallException('updateOne() does not support a skip offset');
        }

        $options = empty($this->writeOptions) ? array() : $this->writeOptions;
        $options['multiple'] = false;
        $options['upsert'] = ($this->upsert === true);

        return $this->collection->update($this->query, $newObj, $options);
    }

    /**
     * Update one document matching the current scope and return its modified
     * value.
     *
     * @param array|object $newObj
     * @return array|boolean
     * @throws BadMethodCallException if the skip offset is greater than zero
     * @throws InvalidArgumentException if $newObj does not contain update operators
     * @throws ResultException if the findAndModify command fails
     */
    public function updateOneAndGet($newObj)
    {
        if ( ! $this->hasUpdateOperator($newObj)) {
            throw new InvalidArgumentException('updateOneAndGet() does not support update operators in $newObj');
        }

        return $this->findAndModify(array('update' => $newObj, 'new' => true));
    }

    /**
     * Set the upsert option for a write operation.
     *
     * @param boolean $upsert
     * @return self
     */
    public function upsert($upsert = true)
    {
        $this->upsert = (boolean) $upsert;

        return $this;
    }

    /**
     * Set the batch size for a read operation.
     *
     * @param integer $batchSize
     * @return self
     */
    public function withBatchSize($batchSize)
    {
        $this->batchSize = (integer) $batchSize;

        return $this;
    }

    /**
     * Set the cursor options.
     *
     * @see http://php.net/manual/en/mongocursor.setflag.php
     * @param array|CursorOptions $cursorOptions
     * @return self
     * @throws InvalidArgumentException if $cursorOptions is neither an array
     *                                  nor a CursorOptions instance
     */
    public function withCursorOptions($cursorOptions)
    {
        if ($cursorOptions instanceof CursorOptions) {
            $cursorOptions = $cursorOptions->toArray();
        }

        if ( ! is_array($cursorOptions)) {
            throw new InvalidArgumentException('$cursorOptions must be an array or CursorOptions instance');
        }

        $this->cursorOptions = $cursorOptions;

        return $this;
    }

    /**
     * Set the query options.
     *
     * @see http://php.net/manual/en/mongocursor.addoption.php
     * @param array|QueryOptions $queryOptions
     * @return self
     * @throws InvalidArgumentException if $queryOptions is neither an array nor
     *                                  a QueryOptions instance
     */
    public function withQueryOptions($queryOptions)
    {
        if ($queryOptions instanceof QueryOptions) {
            $queryOptions = $queryOptions->toArray();
        }

        if ( ! is_array($queryOptions)) {
            throw new InvalidArgumentException('$queryOptions must be an array or QueryOptions instance');
        }

        $this->queryOptions = $queryOptions;

        return $this;
    }

    /**
     * Set the read preference.
     *
     * @see http://docs.mongodb.org/manual/core/read-preference/
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
     * Set the write options.
     *
     * @see http://docs.mongodb.org/manual/core/write-concern/
     * @param array|WriteOptions $writeOptions
     * @return self
     * @throws InvalidArgumentException if $writeOptions is neither an array nor
     *                                  a WriteOptions instance
     */
    public function withWriteOptions($writeOptions)
    {
        if ($writeOptions instanceof WriteOptions) {
            $writeOptions = $writeOptions->toArray();
        }

        if ( ! is_array($writeOptions)) {
            throw new InvalidArgumentException('$writeOptions must be an array or WriteOptions instance');
        }

        $this->writeOptions = $writeOptions;

        return $this;
    }

    /**
     * Execute a database command.
     *
     * @param array $command
     * @return array
     * @throws MongoResultException if the command fails
     */
    private function command(array $command)
    {
        $result = $this->collection->db->command($command);

        if ( ! isset($result['ok'])) {
            throw new UnexpectedValueException('Command response missing "ok" field');
        }

        if ($result['ok'] < 1) {
            throw new ResultException($result);
        }

        return $result;
    }

    /**
     * Create a MongoCursor based on the Scope's current state.
     *
     * @return \MongoCursor
     */
    private function createCursor()
    {
        $cursor = $this->collection->find($this->query, $this->fields);

        foreach ($this->cursorOptions as $flag => $bit) {
            $cursor->setFlag((integer) $flag, (boolean) $bit);
        }

        foreach ($this->queryOptions as $option => $value) {
            $cursor->addOption($option, $value);
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
     * Execute a findAndModify command.
     *
     * @param array $options
     * @return array|null
     * @throws MongoResultException if the command fails
     * @throws BadMethodCallException if the skip offset is greater than zero
     */
    private function findAndModify(array $options)
    {
        if ($this->skip > 0) {
            throw new BadMethodCallException('findAndModify command does not support a skip offset');
        }

        $command = array_merge(
            array('findAndModify' => $this->collection->getName()),
            $options
        );

        if ( ! empty($this->query)) {
            $command['query'] = $this->query;
        }

        if ( ! empty($this->fields)) {
            $command['fields'] = $this->fields;
        }

        if ( ! empty($this->sort)) {
            $command['sort'] = $this->sort;
        }

        $result = $this->command($command);

        return $result['value'];
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
