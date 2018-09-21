<?php

namespace TractorCow\Fluent\Extension;

use InvalidArgumentException;
use SilverStripe\CMS\Model\SiteTree;
use SilverStripe\Core\Config\Config;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DataQuery;
use SilverStripe\ORM\DB;
use SilverStripe\ORM\Queries\SQLExpression;
use SilverStripe\ORM\Queries\SQLSelect;
use SilverStripe\Versioned\Versioned;
use TractorCow\Fluent\Model\Locale;
use TractorCow\Fluent\State\FluentState;

/**
 * Extension for versioned localised objects
 *
 * Important: If adding this to a custom object, this extension must be added AFTER the versioned extension.
 * Use yaml `after` to enforce this
 */
class FluentVersionedExtension extends FluentExtension
{
    /**
     * Live table suffix
     */
    const SUFFIX_LIVE = '_Live';

    /**
     * Versions table suffix
     */
    const SUFFIX_VERSIONS = '_Versions';

    /**
     * Default version table fields. _Versions has extra Version column.
     *
     * @var array
     */
    protected $defaultVersionsFields = [
        'Version' => 'Int',
    ];

    /**
     * Default version table indexes, including unique index to include Version column.
     *
     * @var array
     */
    protected $defaultVersionsIndexes = [
        'Fluent_Record' => [
            'type' => 'unique',
            'columns' => [
                'RecordID',
                'Locale',
                'Version',
            ],
        ],
    ];

    /**
     * Cache of published status of this record
     *
     * @var array
     */
    protected $localisedStageCache = [];

    /**
     * Array of objectIds keyed by table (ie. stage) and locale. Indicates which object IDs exist in which locales.
     *
     * static::$idsInLocaleCache[ $locale ][ $table(.self::SUFFIX_LIVE) ][ $objectId ] === true
     *
     * @var string[][][]
     */
    protected static $idsInLocaleCache = [];

    /**
     * @config
     * @var array
     */
    private static $prepopulate_locale_object_types = [];

    protected function augmentDatabaseDontRequire($localisedTable)
    {
        DB::dont_require_table($localisedTable);
        DB::dont_require_table($localisedTable . self::SUFFIX_LIVE);
        DB::dont_require_table($localisedTable . self::SUFFIX_VERSIONS);
    }

    protected function augmentDatabaseRequireTable($localisedTable, $fields, $indexes)
    {
        DB::require_table($localisedTable, $fields, $indexes, false);

        // _Live record
        DB::require_table($localisedTable . self::SUFFIX_LIVE, $fields, $indexes, false);

        // Merge fields and indexes with Fluent defaults
        $versionsFields = array_merge($this->defaultVersionsFields, $fields);
        $versionsIndexes = array_merge($indexes, $this->defaultVersionsIndexes);

        DB::require_table($localisedTable . self::SUFFIX_VERSIONS, $versionsFields, $versionsIndexes, false);
    }

    /**
     * {@inheritDoc}
     *
     * @throws InvalidArgumentException if an invalid versioned mode is provided
     */
    public function augmentSQL(SQLSelect $query, DataQuery $dataQuery = null)
    {
        /** @var Locale|null $locale */
        $locale = $this->getDataQueryLocale($dataQuery);
        if (!$locale) {
            return;
        }

        // Rewrite query un-versioned
        parent::augmentSQL($query, $dataQuery);

        // Rewrite based on versioned rules
        if (!$dataQuery->getQueryParam('Versioned.mode')) {
            return;
        }

        $tables = $this->getLocalisedTables();
        $versionedMode = $dataQuery->getQueryParam('Versioned.mode');
        switch ($versionedMode) {
            // Reading a specific stage (Stage or Live)
            case 'stage':
            case 'stage_unique':
                // Rename all localised tables (note: alias remains unchanged). This is only done outside of draft.
                $stage = $dataQuery->getQueryParam('Versioned.stage');
                if ($stage !== Versioned::DRAFT) {
                    $this->renameLocalisedTables($query, $tables);
                }
                break;
            // Return all version instances
            case 'archive':
            case 'all_versions':
            case 'latest_versions':
            case 'version':
                $this->rewriteVersionedTables($query, $tables, $locale);
                break;
            default:
                throw new InvalidArgumentException("Bad value for query parameter Versioned.mode: {$versionedMode}");
        }
    }

    /**
     * Rewrite all joined tables
     *
     * @param SQLSelect $query
     * @param array $tables
     * @param Locale $locale
     */
    protected function rewriteVersionedTables(SQLSelect $query, array $tables, Locale $locale)
    {
        foreach ($tables as $tableName => $fields) {
            // Rename to _Versions suffixed versions
            $localisedTable = $this->getLocalisedTable($tableName);
            $query->renameTable($localisedTable, $localisedTable . self::SUFFIX_VERSIONS);

            // Add the chain of locale fallbacks
            $this->addLocaleFallbackChain($query, $tableName, $locale);
        }
    }

    /**
     * Update all joins to include Version as well as Locale / Record
     *
     * @param SQLSelect $query
     * @param string $tableName
     * @param Locale $locale
     */
    protected function addLocaleFallbackChain(SQLSelect $query, $tableName, Locale $locale)
    {
        $baseTable = $this->owner->baseTable();

        foreach ($locale->getChain() as $joinLocale) {
            /** @var Locale $joinLocale */
            $joinAlias = $this->getLocalisedTable($tableName, $joinLocale->Locale);
            $versionTable = $baseTable . self::SUFFIX_VERSIONS;

            $query->setJoinFilter(
                $joinAlias,
                "\"{$versionTable}\".\"RecordID\" = \"{$joinAlias}\".\"RecordID\" "
                . "AND \"{$joinAlias}\".\"Locale\" = ? "
                . "AND \"{$joinAlias}\".\"Version\" = \"{$versionTable}\".\"Version\""
            );
        }
    }

    /**
     * Rename all localised tables to the "live" equivalent name (note: alias remains unchanged)
     *
     * @param SQLSelect $query
     * @param array $tables
     */
    protected function renameLocalisedTables(SQLSelect $query, array $tables)
    {
        foreach ($tables as $table => $fields) {
            $localisedTable = $this->getLocalisedTable($table);
            $query->renameTable($localisedTable, $localisedTable . self::SUFFIX_LIVE);
        }
    }

    /**
     * Apply versioning to write
     *
     * @param array $manipulation
     */
    public function augmentWrite(&$manipulation)
    {
        parent::augmentWrite($manipulation);

        // Only rewrite if the locale is valid
        $locale = Locale::getCurrentLocale();
        if (!$locale) {
            return;
        }

        // Get all tables to translate fields for, and their respective field names
        $includedTables = $this->getLocalisedTables();
        foreach ($includedTables as $table => $localisedFields) {
            // Localise both _Versions and _Live writes
            foreach ([self::SUFFIX_LIVE, self::SUFFIX_VERSIONS] as $suffix) {
                $versionedTable = $table . $suffix;
                $localisedTable = $this->getLocalisedTable($table) . $suffix;

                // Add extra case for "Version" column when localising Versions
                $localisedVersionFields = $localisedFields;
                if ($suffix === self::SUFFIX_VERSIONS) {
                    $localisedVersionFields = array_merge(
                        $localisedVersionFields,
                        array_keys($this->defaultVersionsFields)
                    );
                }

                // Rewrite manipulation
                $this->localiseManipulationTable(
                    $manipulation,
                    $versionedTable,
                    $localisedTable,
                    $localisedVersionFields,
                    $locale
                );
            }
        }
    }

    /**
     * Decorate table to delete with _Live suffix as necessary
     *
     * @param string $tableName
     * @param string $locale
     * @return string
     */
    protected function getDeleteTableTarget($tableName, $locale = '')
    {
        // Rewrite to _Live when deleting from live / unpublishing
        $table = parent::getDeleteTableTarget($tableName, $locale);
        if (Versioned::get_stage() === Versioned::LIVE) {
            $table .= self::SUFFIX_LIVE;
        }
        return $table;
    }

    /**
     * Check if this record is saved in this locale
     *
     * @param string $locale
     * @return bool
     */
    public function isDraftedInLocale($locale = null)
    {
        return $this->isLocalisedInStage(Versioned::DRAFT, $locale);
    }

    /**
     * Check if this record is published in this locale
     *
     * @param string $locale
     * @return bool
     */
    public function isPublishedInLocale($locale = null)
    {
        return $this->isLocalisedInStage(Versioned::LIVE, $locale);
    }

    /**
     * Check if this record exists (in either state) in this locale
     *
     * @param string $locale
     * @return bool
     */
    public function existsInLocale($locale = null)
    {
        return $this->isDraftedInLocale($locale) || $this->isPublishedInLocale($locale);
    }

    /**
     * Check to see whether or not a record exists for a specific Locale in a specific stage.
     *
     * @param string $stage Version stage
     * @param string $locale Locale to check. Defaults to current locale.
     * @return bool
     */
    protected function isLocalisedInStage($stage, $locale = null)
    {
        // Get locale
        if (!$locale) {
            $locale = FluentState::singleton()->getLocale();

            // Potentially no Locales have been created in the system yet.
            if (!$locale) {
                return false;
            }
        }

        $ownerClass = get_class($this->owner);
        $prepopulateClasses = Config::inst()->get(self::class, 'prepopulate_locale_object_types');
        if (is_array($prepopulateClasses) && in_array($ownerClass, $prepopulateClasses)) {
            self::prepoulateIdsInLocale($locale, $ownerClass);
        }

        // Get table
        $baseTable = $this->owner->baseTable();
        $table = $this->getLocalisedTable($baseTable);
        if ($stage === Versioned::LIVE) {
            $table .= self::SUFFIX_LIVE;
        }

        if (isset(self::$idsInLocaleCache[$locale][$table])) {
            return in_array($this->owner->ID, self::$idsInLocaleCache[$locale][$table]);
        }

        // Check cache
        $key = $table . '/' . $locale . '/' . $this->owner->ID;
        if (isset($this->localisedStageCache[$key])) {
            return $this->localisedStageCache[$key];
        }

        $query = new SQLSelect(
            '"ID"'
        );
        $query->addFrom('"'. $table . '"');
        $query->addWhere([
            '"RecordID"' => $this->owner->ID,
            '"Locale"' => $locale,
        ]);

        $result = $query->firstRow()->execute()->value() !== null;

        // Set cache
        $this->localisedStageCache[$key] = $result;
        return $result;
    }

    public function flushCache()
    {
        $this->localisedStageCache = [];
    }

    /**
     * Prepopulate the cache of
     *
     * @param string $locale
     * @param string $dataObject
     */
    public static function prepoulateIdsInLocale($locale, $dataObjectClass, $populateLive = true, $populateDraft = true)
    {
        // Get the table for the given DataObject class
        $self = new static();
        $table = $self->getLocalisedTable(Injector::inst()->get($dataObjectClass)->baseTable());

        // If we already have items then we've been here before...
        if (isset(self::$idsInLocaleCache[$locale][$table])) {
            return;
        }

        $tables = [];
        if ($populateDraft) {
            $tables[] = $table;
        }
        if ($populateLive) {
            $tables[] = $table . self::SUFFIX_LIVE;
        }

        // Populate both the draft and live stages
        foreach ($tables as $table) {
            /** @var SQLSelect $select */
            $select = SQLSelect::create(
                ['"RecordID"'],
                '"' . $table . '"',
                ['Locale' => $locale]
            );
            $result = $select->execute();
            $ids = $result->column('RecordID');

            // We need to execute ourselves as the param is lost from the subSelect
            self::$idsInLocaleCache[$locale][$table] = $ids;
        }
    }
}
