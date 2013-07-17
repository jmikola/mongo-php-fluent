<?php

namespace MongoDB\Options;

use ArrayAccess;

abstract class Options implements ArrayAccess
{
    protected $options = array();

    /**
     * Checks if an option exists.
     *
     * @see http://php.net/manual/en/arrayaccess.offsetexists.php
     * @param string $name
     * @return boolean
     */
    public function offsetExists($name)
    {
        return array_key_exists($name, $this->options);
    }

    /**
     * Gets an option.
     *
     * @see http://php.net/manual/en/arrayaccess.offsetget.php
     * @param string $name
     * @return mixed
     */
    public function offsetGet($name)
    {
        return $this->options[$name];
    }

    /**
     * Sets an option.
     *
     * @see http://php.net/manual/en/arrayaccess.offsetset.php
     * @param string $name
     * @param mixed $value
     */
    public function offsetSet($name, $value)
    {
        $this->options[$name] = $value;
    }

    /**
     * Unsets an option.
     *
     * @see http://php.net/manual/en/arrayaccess.offsetunset.php
     * @param string $name
     */
    public function offsetUnset($name)
    {
        unset($this->options[$name]);
    }

    /**
     * Sets an option.
     *
     * @param string $name
     * @param mixed $value
     * @return self
     */
    public function set($name, $value)
    {
        $this->options[$name] = $value;

        return $this;
    }

    /**
     * Return the options array.
     *
     * @return array
     */
    public function toArray()
    {
        return $this->options;
    }
}
