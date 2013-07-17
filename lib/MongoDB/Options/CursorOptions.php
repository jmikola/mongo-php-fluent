<?php

namespace MongoDB\Options;

use BadMethodCallException;

/**
 * Cursor options are wire protocol flags for OP_QUERY messages.
 *
 * @see http://php.net/manual/en/mongocursor.setflag.php
 * @see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#MongoWireProtocol-OPQUERY
 */
class CursorOptions extends Options
{
    const FLAG_TAILABLE = 1;
    const FLAG_SLAVE_OK = 2;
    const FLAG_OPLOG_REPLAY = 3;
    const FLAG_NO_CURSOR_TIMEOUT = 4;
    const FLAG_AWAIT_DATA = 5;
    const FLAG_EXHAUST = 6;
    const FLAG_PARTIAL = 7;

    /**
     * Set the awaitData cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function awaitData($bit = true)
    {
        $this->options[self::FLAG_AWAIT_DATA] = (boolean) $bit;

        return $this;
    }

    /**
     * Set the exhaust cursor option.
     *
     * @param boolean $bit
     * @throws BadMethodCallException
     */
    public function exhaust($bit = true)
    {
        throw new BadMethodCallException('PHP driver does not support the "exhaust" cursor option.');
    }

    /**
     * Set the noCursorTimeout cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function noCursorTimeout($bit = true)
    {
        $this->options[self::FLAG_NO_CURSOR_TIMEOUT] = (boolean) $bit;

        return $this;
    }

    /**
     * Set the oplogReplay cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function oplogReplay($bit = true)
    {
        $this->options[self::FLAG_OPLOG_REPLAY] = (boolean) $bit;

        return $this;
    }

    /**
     * Set the partial cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function partial($bit = true)
    {
        $this->options[self::FLAG_PARTIAL] = (boolean) $bit;

        return $this;
    }

    /**
     * Set the slaveOk cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function slaveOk($bit = true)
    {
        $this->options[self::FLAG_SLAVE_OK] = (boolean) $bit;

        return $this;
    }

    /**
     * Set the tailable cursor option.
     *
     * @param boolean $bit
     * @return self
     */
    public function tailable($bit = true)
    {
        $this->options[self::FLAG_TAILABLE] = (boolean) $bit;

        return $this;
    }
}
