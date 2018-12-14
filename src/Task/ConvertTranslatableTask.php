<?php

namespace TractorCow\Fluent\Task;

use SilverStripe\Core\ClassInfo;
use SilverStripe\Dev\BuildTask;
use SilverStripe\Dev\Debug;
use SilverStripe\i18n\i18n;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DB;
use SilverStripe\ORM\Queries\SQLSelect;
use SilverStripe\Security\DefaultAdminService;
use SilverStripe\Security\Member;
use SilverStripe\Versioned\Versioned;
use TractorCow\Fluent\Extension\FluentExtension;
use TractorCow\Fluent\Extension\FluentFilteredExtension;
use TractorCow\Fluent\Model\Locale;
use TractorCow\Fluent\State\FluentState;
use TractorCow\Fluent\Task\ConvertTranslatableTask\Exception;

/**
 * Provides migration from the Translatable module in a SilverStripe 3 website to the Fluent format for SilverStripe 4.
 * This task assumes that you have upgraded your website to run on SilverStripe 4 already, and you want to migrate the
 * existing data from your project into a format that is compatible with Fluent.
 *
 * Don't forget to:
 *
 * 1. Back up your DB
 * 2. dev/build
 * 3. Log into the CMS and set up the locales you want to use
 * 4. Back up your DB again
 * 5. Log into the CMS and check everything
 */
class ConvertTranslatableTask extends BuildTask
{
    protected $title = "Convert Translatable > Fluent Task";

    protected $description = "Migrates site DB from SS3 Translatable DB format to SS4 Fluent.";

    private static $segment = 'ConvertTranslatableTask';

    /**
     * Gets all classes with FluentExtension
     *
     * @return array Array of classes to migrate
     */
    public function fluentClasses()
    {
        $classes = [];
        $dataClasses = ClassInfo::subclassesFor(DataObject::class);
        array_shift($dataClasses);
        foreach ($dataClasses as $class) {
            $base = DataObject::getSchema()->baseDataClass($class);
            foreach (DataObject::get_extensions($base) as $extension) {
                if (is_a($extension, FluentExtension::class, true)) {
                    $classes[] = $base;
                    break;
                }
            }
        }
        return array_unique($classes);
    }

    public function run($request)
    {
        $this->checkInstalled();

        // we may need some privileges for this to work
        // without this, running under sake is a problem
        // maybe sake could take care of it ...
        Member::actAs(
            DefaultAdminService::singleton()->findOrCreateDefaultAdmin(),
            function () {
                DB::get_conn()->withTransaction(function () {
                    $defaultLocale = i18n::config()->get('default_locale');
                    Versioned::set_stage(Versioned::DRAFT);
                    $classes = $this->fluentClasses();
                    $tables = DB::get_schema()->tableList();
                    $deletionTables = [];
                    if (empty($classes)) {
                        Debug::message('No classes have Fluent enabled, so skipping.', false);
                    }

                    foreach ($classes as $class) {
                        /** @var DataObject $class */

                        // Ensure that a translationgroup table exists for this class
                        $baseTable = DataObject::getSchema()->baseDataTable($class);

                        // Tables that have to be corrected to unify all the translations under one base record
                        $postProcessTables = [
                            $baseTable . '_Localised',
                            $baseTable . '_Localised_Live',
                            $baseTable . '_Versions',
                        ];

                        // Flag tables to prune old translations from
                        $deletionTables[$baseTable] = 1;
                        $deletionTables[$baseTable . '_Versions'] = 1;
                        $deletionTables[$baseTable . '_Live'] = 1;

                        $groupTable = strtolower($baseTable . "_translationgroups");
                        if (isset($tables[$groupTable])) {
                            $groupTable = $tables[$groupTable];
                        } else {
                            Debug::message("Ignoring class without _translationgroups table ${class}", false);
                            continue;
                        }

                        // Disable filter if it has been applied to the class
                        if (singleton($class)->hasMethod('has_extension')
                            && $class::has_extension(FluentFilteredExtension::class)
                        ) {
                            $class::remove_extension(FluentFilteredExtension::class);
                        }

                        // Get all of Translatable's translation group IDs
                        $translation_groups = DB::query(sprintf('SELECT DISTINCT TranslationGroupID FROM %s', $groupTable));
                        foreach ($translation_groups as $translation_group) {
                            $translationGroupSet = [];
                            $translationGroupID = $translation_group['TranslationGroupID'];
                            $itemIDs = DB::query(sprintf("SELECT OriginalID FROM %s WHERE TranslationGroupID = %d", $groupTable, $translationGroupID))->column();
                            $instances = $class::get()->sort('Created')->byIDs($itemIDs);
                            if (!$instances->count()) {
                                continue;
                            }
                            Debug::message(sprintf("%d instances for %s: [%s]\n", $instances->count(), implode(', ', $itemIDs), implode(', ', $instances->column())), false);
                            foreach ($instances as $instance) {
                                /** @var DataObject $instance */

                                // Get the Locale column directly from the base table, because the SS ORM will set it to the default
                                $instanceLocale = SQLSelect::create()
                                    ->setFrom("\"{$baseTable}\"")
                                    ->setSelect('"Locale"')
                                    ->setWhere(["\"{$baseTable}\".\"ID\"" => $instance->ID])
                                    ->execute()
                                    ->first();

                                // Ensure that we got the Locale out of the base table before continuing
                                if (empty($instanceLocale['Locale'])) {
                                    Debug::message("Skipping {$instance->Title} with ID {$instance->ID} - couldn't find Locale", false);
                                    continue;
                                }

                                // Check for obsolete classes that don't need to be handled any more
                                if ($instance->ObsoleteClassName) {
                                    Debug::message(
                                        "Skipping {$instance->ClassName} with ID {$instance->ID} because it from an obsolete class",
                                        false
                                    );
                                    continue;
                                }
                                $instanceLocale = $instanceLocale['Locale'];
                                $translationGroupSet[$instanceLocale] = $instance;
                            }
                            if (empty($translationGroupSet)) {
                                continue;
                            }

                            // Now write out the records for the translation group
                            if (array_key_exists($defaultLocale, $translationGroupSet)) {
                                $originalRecordID = $translationGroupSet[$defaultLocale]->ID;
                            } else {
                                $originalRecordID = reset($translationGroupSet)->ID; // Just use the first one
                            }
                            foreach ($translationGroupSet as $locale => $instance) {
                                Debug::message(
                                    "Updating {$instance->ClassName} {$instance->Title} ({$instance->ID}) [RecordID: {$originalRecordID}] with locale {$locale}",
                                    false
                                );
                                FluentState::singleton()
                                    ->withState(function (FluentState $state) use ($instance, $locale, $originalRecordID) {
                                        // Use Fluent's ORM to write and/or publish the record into the correct locale
                                        // from Translatable
                                        $state->setLocale($locale);
                                        if (!$this->isPublished($instance)) {
                                            $instance->write();
                                            Debug::message("  --  Saved to draft", false);
                                        } elseif ($instance->publishRecursive() === false) {
                                            Debug::message("  --  Publishing FAILED", false);
                                            throw new Exception("Failed to publish");
                                        } else {
                                            Debug::message("  --  Published", false);
                                        }
                                    });
                            }
                            foreach ($postProcessTables as $table) {
                                $query = sprintf('UPDATE %s SET RecordID = %d WHERE RecordID IN (%s)', $table, $originalRecordID, implode(', ', $itemIDs));
                                Debug::message($query, false);
                                DB::query($query);
                            }

                        }

                        // Delete old base items that don't have the default locale
                        foreach (array_keys($deletionTables) as $table) {
                            $query = sprintf("DELETE FROM %s WHERE Locale != '%s'", $table, $defaultLocale);
                            Debug::message($query, false);
                            DB::query($query);
                        }

                        // Drop the "Locale" column from the base table
                        Debug::message('Dropping "Locale" column from ' . $baseTable, false);
                        DB::query(sprintf('ALTER TABLE "%s" DROP COLUMN "Locale"', $baseTable));

                        // Drop the "_translationgroups" translatable table
                        Debug::message('Deleting Translatable table ' . $groupTable, false);
                        DB::query(sprintf('DROP TABLE IF EXISTS "%s"', $groupTable));
                    }
                });
            }
        );
    }

    /**
     * Checks that fluent is configured correctly
     *
     * @throws ConvertTranslatableTask\Exception
     */
    protected function checkInstalled()
    {
        // Assert that fluent is configured
        $locales = Locale::getLocales();
        if (empty($locales)) {
            throw new Exception("Please configure Fluent locales (in the CMS) prior to migrating from translatable");
        }

        $defaultLocale = Locale::getDefault();
        if (empty($defaultLocale)) {
            throw new Exception(
                "Please configure a Fluent default locale (in the CMS) prior to migrating from translatable"
            );
        }
    }

    /**
     * Determine whether the record has been published previously/is currently published
     *
     * @param DataObject $instance
     * @return bool
     */
    protected function isPublished(DataObject $instance)
    {
        $isPublished = false;
        if ($instance->hasMethod('isPublished')) {
            $isPublished = $instance->isPublished();
        }
        return $isPublished;
    }
}
