<?php

namespace MongoDB\Options;

/**
 * Write options specify the write concern for write operations.
 *
 * @see http://docs.mongodb.org/manual/core/write-concern/
 */
class WriteOptions extends Options
{
    /**
     * Set the fsync option.
     *
     * @param boolean $fsync
     * @return self
     */
    public function fsync($fsync = true)
    {
        $this->options['fsync'] = (boolean) $fsync;

        return $this;
    }

    /**
     * Set the journal option.
     *
     * @param boolean $journal
     * @return self
     */
    public function journal($journal = true)
    {
        $this->options['j'] = (boolean) $journal;

        return $this;
    }

    /**
     * Set the write concern option.
     *
     * @param integer|string $writeConcern
     * @return self
     */
    public function writeConcern($writeConcern)
    {
        $this->options['w'] = $writeConcern;

        return $this;
    }

    /**
     * Set the write concern timeout option.
     *
     * @param integer $timeout Write acknowledgement timeout (milliseconds)
     * @return self
     */
    public function writeConcernTimeout($timeout)
    {
        $this->options['wtimeout'] = (integer) $timeout;

        return $this;
    }
}
