<?php

namespace MongoDB\Options;

/**
 * Query options are query document elements in OP_QUERY messages.
 *
 * @see http://php.net/manual/en/mongocursor.addoption.php
 * @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#MongoWireProtocol-OPQUERY
 */
class QueryOptions extends Options
{
    /**
     * Set the $comment query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/comment/
     * @param string $comment
     * @return self
     */
    public function comment($comment)
    {
        $this->options['$comment'] = (string) $comment;

        return $this;
    }

    /**
     * Set the $hint query option.
     *
     * @param array|string $index
     * @return self
     */
    public function hint($index)
    {
        $this->options['$hint'] = is_array($hint) ? $hint : (string) $hint;

        return $this;
    }

    /**
     * Set the $max query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/max/
     * @param array $max
     * @return self
     */
    public function max(array $max)
    {
        $this->options['$max'] = $max;

        return $this;
    }

    /**
     * Set the $maxScan query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/maxScan/
     * @param integer $maxScan
     * @return self
     */
    public function maxScan($maxScan)
    {
        $this->options['$maxScan'] = (integer) $maxScan;

        return $this;
    }

    /**
     * Set the $maxTime cursor.
     *
     * @param integer $maxTime
     * @return self
     */
    public function maxTime($maxTime)
    {
        $this->options['$maxTime'] = (integer) $maxTime;

        return $this;
    }

    /**
     * Set the $min query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/min/
     * @param array $min
     * @return self
     */
    public function min(array $min)
    {
        $this->options['$min'] = $min;

        return $this;
    }

    /**
     * Set the $returnKey query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/returnKey/
     * @param boolean $returnKey
     * @return self
     */
    public function returnKey($returnKey = true)
    {
        $this->options['$returnKey'] = (boolean) $returnKey;

        return $this;
    }

    /**
     * Set the $showDiskLoc query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/showDiskLoc/
     * @param boolean $showDiskLoc
     * @return self
     */
    public function showDiskLoc($showDiskLoc = true)
    {
        $this->options['$showDiskLoc'] = (boolean) $showDiskLoc;

        return $this;
    }

    /**
     * Set the $snapshot query option.
     *
     * @see http://docs.mongodb.org/manual/reference/operator/snapshot/
     * @param boolean $snapshot
     * @return self
     */
    public function snapshot($snapshot = true)
    {
        $this->options['$snapshot'] = (boolean) $snapshot;

        return $this;
    }
}
