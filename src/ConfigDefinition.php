<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
            ->scalarNode('#token')->isRequired()->cannotBeEmpty()->end()
            ->scalarNode('url')->isRequired()->cannotBeEmpty()->end()
            ->scalarNode('tableName')->defaultValue("")->end()
            ->scalarNode('changedSince')->defaultValue("")->end()
            ->booleanNode('extractMetadata')->defaultFalse()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
