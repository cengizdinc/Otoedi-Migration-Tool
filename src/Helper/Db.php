<?php

namespace Otoedi\Migration\Helper;

use Laminas\Db\Adapter\Adapter;
use Laminas\Db\Sql\Sql;
use Laminas\Db\Adapter\Exception\InvalidQueryException;
use Laminas\Db\Sql\Delete;
use Laminas\Db\Sql\Insert;
use Laminas\Db\Sql\Select;
use Laminas\Db\Sql\Update;
use Laminas\Db\Sql\Where;

/**
 * Class Db
 * @package Otoedi\Migration\Helper
 */
class Db
{

    /**
     * @var Adapter
     */
    public Adapter $adapter;
    /**
     * @var Sql
     */
    public Sql $sql;

    /**
     * Db constructor.
     * @param array $adapter
     */
    public function __construct(array $adapter)
    {
        $this->adapter = new Adapter($adapter);
    }

    /**
     * @param $table
     * @param array $values
     * @param false $columns
     * @return mixed|null
     */
    public function create($table, array $values, $columns = false)
    {
        $sql = new Sql($this->adapter);
        $insert = new Insert($table);

        if (false !== $columns) {
            $values = array_combine($columns, $values);
        }

        $insert->values($values);
        $statement = $this->adapter->query($sql->buildSqlString($insert))->execute();

        return $statement->getGeneratedValue();
    }

    /**
     * @param string $table
     * @param array $values
     * @param $where
     * @return false|int
     */
    public function update(string $table, array $values, $where)
    {
        $sql = new Sql($this->adapter);
        $update = new Update($table);
        $update->set($values);

        if (false !== $where && $where instanceof Where) {
            $update->where($where);
        }

        try {
            $statement = $this->adapter->query($sql->buildSqlString($update))->execute();
        } catch (InvalidQueryException $exception) {
            return false;
        }

        return $statement->getAffectedRows();
    }

    /**
     * @param string $table
     * @param false $where
     * @return false|int
     */
    public function delete(string $table, $where = false)
    {
        $sql = new Sql($this->adapter);
        $delete = new Delete($table);

        if ($where !== false and ($where instanceof Where or is_array($where))) {
            $delete->where($where);
        }

        try {
            $statement = $this->adapter->query($sql->buildSqlString($delete))->execute();
        } catch (InvalidQueryException $exception) {
            return false;
        }

        return $statement->getAffectedRows();
    }

    /**
     * @param string $table
     * @param string $alias
     * @param array $columns
     * @param false $where
     * @param false $limit
     * @param array $sorting
     * @param array $joins
     * @param array $group
     * @return array|false|mixed
     */
    public function get(string $table, string $alias, array $columns = [], $where = false, $limit = false, $sorting = [], array $joins = [], $group = [])
    {
        $sql = new Sql($this->adapter);
        $select = new Select([$alias => $table]);

        if (is_array($columns) && count($columns) > 0) {
            $select->columns($columns);
        }

        if ($where !== false and ($where instanceof Where or is_array($where))) {
            $select->where($where);
        }

        if (!empty($joins)) {
            foreach ($joins as $join) {
                if (isset($join['columns']) and is_null($join['columns'])) {
                    $join['columns'] = Select::SQL_STAR;
                }
                if (empty($join['type'])) {
                    $join['type'] = Select::JOIN_INNER;
                }

                $select->join($join['name'], $join['on'], $join['columns'], $join['type']);
            }
        }

        if (false !== $limit and !is_array($limit) && $limit * 1 > 0) {
            $select->limit($limit);
        }
        if (is_array($limit) && isset($limit['limit'])) {
            $select->limit($limit['limit'])
                ->offset(@$limit['offset'] * 1);
        }
        if (!empty($sorting)) {
            $select->order($sorting);
        }

        if (!empty($group)) {
            $select->group($group);
        }

        // echo $sql->buildSqlString($select);

        try {
            $statement = $this->adapter->query($sql->buildSqlString($select), Adapter::QUERY_MODE_EXECUTE);
        } catch (InvalidQueryException $exception) {
            return false;
        }

        $resultset = $statement->toArray();

        return count($resultset) > 1 ? $resultset : (!empty($resultset[0]) ? $resultset[0] : []);
    }
}
