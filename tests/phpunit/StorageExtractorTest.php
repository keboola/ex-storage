<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor\Tests;

use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageExtractor\Component;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageExtractorTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('KBC_TEST_TOKEN')) || empty(getenv('KBC_TEST_WRITE_TOKEN')) ||
                empty(getenv('KBC_TEST_URL')) || empty(getenv('KBC_TEST_BUCKET'))
        ) {
            throw new \Exception('KBC_TEST_TOKEN, KBC_TEST_WRITE_TOKEN, KBC_TEST_URL or KBC_TEST_BUCKET is empty');
        }
        $this->client = new Client([
            'token' => getenv('KBC_TEST_WRITE_TOKEN'),
            'url' => getenv('KBC_TEST_URL'),
        ]);
        $tables = $this->client->listTables(getenv('KBC_TEST_BUCKET'));
        foreach ($tables as $table) {
            $this->client->dropTable($table['id']);
        }
    }

    public function testBasic(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-1', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-1.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'name'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv.manifest');
        $data = json_decode(file_get_contents($baseDir . '/out/tables/some-table-1.csv.manifest'), true);
        self::assertEquals(['metadata' => [], 'column_metadata' => ['id' => [], 'name' => []]], $data);
    }

    public function testInvalidToken(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->run();
    }

    public function testInvalidTokenPermissions(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_WRITE_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('The token does not have only read permissions to the bucket in.c-ex-storage');
        $app->run();
    }

    public function testTableFilter(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"foo\"\n\"1\",\"aa\"\n\"2\",\"bb\"\n\"3\",\"cc\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-2', $csv, ['primaryKey' => 'id']);
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"bar\"\n\"1\",\"x\"\n\"2\",\"y\"\n\"3\",\"z\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-3', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tables' => ['some-table-3'],
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertFileExists($baseDir . '/out/tables/some-table-3.csv');
        self::assertFileNotExists($baseDir . '/out/tables/some-table-2.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-3.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'x'],
                ['2', 'y'],
                ['3', 'z'],
                ['id', 'bar'],
            ],
            $rows
        );
        self::assertFileNotExists($baseDir . '/out/tables/some-table-2.csv.manifest');
        self::assertFileExists($baseDir . '/out/tables/some-table-3.csv.manifest');
        $data = json_decode(file_get_contents($baseDir . '/out/tables/some-table-3.csv.manifest'), true);
        self::assertEquals(
            [
                'metadata' => [],
                'column_metadata' => [
                    'id' => [],
                    'bar' => [],
                ],
            ],
            $data
        );
    }

    public function testTableInvalidFilter(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tables' => ['non-existent-table'],
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectExceptionMessage('The table non-existent-table not found in the bucket in.c-ex-storage');
        self::expectException(UserException::class);
        $app->run();
    }

    public function testTableMetadata(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"foo\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-4', $csv, ['primaryKey' => 'id']);
        $metadata = new Metadata($this->client);
        $metadata->postTableMetadata(
            getenv('KBC_TEST_BUCKET') . '.some-table-4',
            'testing',
            [
                [
                    'key' => 'some-key',
                    'value' => 'some-value',
                ],
            ]
        );
        $metadata->postColumnMetadata(
            getenv('KBC_TEST_BUCKET') . '.some-table-4.foo',
            'testing',
            [
                [
                    'key' => 'another-key',
                    'value' => 'another-value',
                    'provider' => 'test',
                ],
            ]
        );

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tables' => ['some-table-4'],
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->run();
        self::assertFileExists($baseDir . '/out/tables/some-table-4.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-4.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'foo'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-4.csv.manifest');
        $data = json_decode(file_get_contents($baseDir . '/out/tables/some-table-4.csv.manifest'), true);
        self::assertEquals(
            [
                'metadata' => [
                    [
                        'key' => 'some-key',
                        'value' => 'some-value',
                    ],
                ],
                'column_metadata' => [
                    'id' => [],
                    'foo' => [
                        [
                            'key' => 'another-key',
                            'value' => 'another-value',
                        ],
                    ],
                ],
            ],
            $data
        );
    }

    public function testAction(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"foo\"\n\"1\",\"aa\"\n\"2\",\"bb\"\n\"3\",\"cc\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-5', $csv, ['primaryKey' => 'id']);
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"bar\"\n\"1\",\"x\"\n\"2\",\"y\"\n\"3\",\"z\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-6', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'list',
        ];
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $result = '';
        ob_start(function ($content) use (&$result) : void {
            $result .= $content;
        });
        $app->run();
        ob_end_clean();
        $data = json_decode($result, true);
        self::assertArrayHasKey('tables', $data);
        sort($data['tables']);
        self::assertEquals(['some-table-5', 'some-table-6'], $data['tables']);
    }

    public function testActionInvalidToken(): void
    {
        $temp = new Temp('ex-storage');
        $temp->initRunFolder();
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();

        $configFile = [
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'list',
        ];
        $fs->dumpFile($baseDir . '/config.json', \GuzzleHttp\json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->run();
    }
}
