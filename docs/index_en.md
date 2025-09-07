---
title: Generic import plugin for JSON files
identifier: intranda_workflow_import_json
description: This generic import plugin allows the bulk import of data from JSON files
published: true
keywords:
    - Goobi workflow
    - Plugin
    - Workflow Plugin
---
## Introduction
This is the technical documentation for the workflow plugin for Goobi workflow, which is used to read metadata from multiple JSON files and create processes based on it.


## Installation
To install the plugin, the following two files must be installed:

```bash
/opt/digiverso/goobi/plugins/workflow/plugin-intranda-workflow-import-json-base.jar
/opt/digiverso/goobi/plugins/GUI/plugin-intranda-workflow-import-json-gui.jar
```

To configure how the plugin should behave, various values can be adjusted in the configuration file. The configuration file is usually located here:

```bash
/opt/digiverso/goobi/config/plugin_intranda_workflow_import_json.xml
```

![Selection of the Workflow plugin](screen1_en.png)

## Configuration
The content of this configuration file is as follows:

```xml
<config_plugin>
	<!-- folder that contains the json files -->
	<jsonFolder>/opt/digiverso/goobi/import/</jsonFolder>

	<!-- folder used to hold the downloaded images temporarily -->
	<importFolder>/opt/digiverso/goobi/tmp/</importFolder>

	<!-- which workflow to use -->
	<workflow>Sample_Workflow</workflow>

	<!-- which publication type to use -->
	<publicationType>Monograph</publicationType>

	<!-- URL types that hold downloadable resources, could be none or multiple -->
	<downloadableUrl>GizaPhotosMain</downloadableUrl>
	<downloadableUrl>GizaPrimaryDisplayMain</downloadableUrl>

	<!-- whether or not to save the partner url, DEFAULT false -->
	<partnerUrl save="true">
		<!-- at most one urlBase should be configured, DEFAULT "" -->
		<urlBase>giza.fas.harvard.edu</urlBase>
		<!-- there could be multiple urlParts-->
		<urlPart>$._type</urlPart>
		<urlPart>$._id</urlPart>
		<!-- at most one urlTail should be configured, DEFAULT "" -->
		<urlTail>full</urlTail>
		<!-- MetadataType that is to hold the value of the partner url -->
		<urlMetadata>GizaPartnerUrl</urlMetadata>
	</partnerUrl>

	<!-- same source could be used multiple times for different MetadataTypes -->
	<metadata source="$._source.id" target="TitleDocMain" />
	<metadata source="$._source.id" target="CatalogIDDigital" />
	<!-- source that does not start with $ or @ will be regarded as value -->
	<metadata source="Giza" target="singleDigCollection" />
	<!-- source that ends with [:] is an array -->
	<metadata source="$._source.allnumbers[:]" target="GizaAllNumbers" />

	<!-- group is for MetadataGroups, there could be multiple -->
	<!-- no need to add the [:] to the end of @source -->
	<!-- @type is the name of the MetadataGroupType -->
	<group source="$._source.sitedates" type="GizaSiteDatesGroup">
		<!-- source that starts with @ will be regarded as relative json path -->
		<metadata source="@.type" target="GizaSiteDateType" />
		<metadata source="@.date" target="GizaSiteDate" />
	</group>

	<!-- @altType is the name of an alternative MetadataGroupType, which will be used to create another MetadataGroup for all items that are filtered out. OPTIONAL. -->
	<!-- @key is the filtering key that is used to retrieve the value from the JSONObject. OPTIONAL. -->
	<!-- @value is used to compare with the value retrieved from the JSONObject. OPTIONAL. DEFAULT "". -->
	<!-- @method defines the logic for comparing values. Options are is, not, startsWith, endsWith, contains. OPTIONAL. DEFAULT 'is'. -->
	<group source="$._source.relateditems.modernpeople" type="ModernPeopleGroup" altType="_ModernPeopleGroupHidden" key="role" value="Excavator" method="is">
		<metadata source="@.role" target="GizaModernPeopleRole" />
		<!-- same source could be used multiple times for different MetadataTypes -->
		<metadata source="@.id" target="GizaModernPeopleId" />
		<metadata source="@.id" target="GizaModernPeopleId2" />
	</group>

	<!-- if no @altType is configured, then all items that are filtered out will not be imported. -->
	<group source="$._source.relateditems.ancientpeople" type="AncientPeopleGroup" key="role" value="Tomb Owner">
		<metadata source="@.role" target="GizaAncientPeopleRole" />
		<metadata source="@.id" target="GizaAncientPeopleId" />
	</group>

	<!-- child is for child DocStructs, there could be multiple -->
	<!-- no need to add the [:] to the end of @source-->
	<!-- the attributes @key, @value, @method are the same as in group -->
	<!-- there is NO @altType attribute for child, all items that are filtered out will not be imported -->
	<child source="$._source.relateditems.photos" type="GizaPhoto" key="number" value="KHM_AEOS_" method="startsWith">
		<!-- same source could be used multiple times for different MetadataTypes -->
		<metadata source="@.drs_id" target="GizaPhotosDrsId" />
		<metadata source="@.drs_id" target="CatalogIDDigital" />
	</child>

</config_plugin>
```

The configuration of the plugin is as follows:

| Value | Description |
| :--- | :--- |
| `jsonFolder` | Path to the directory where the JSON files are located. |
| `importFolder` | Path to the directory where the images to be downloaded are to be saved before they are imported into the processes. |
| `workflow` | Name of the production template to be used. |
| `publicationType` | Publication type for the transactions to be created. |
| `downloadableUrl` | Type of metadata in which the URL from which the images are to be downloaded is specified. This value is repeatable. |
| `metadata` | A metadata object is generated from each tag. The attribute `@source` refers to a JSON path if it begins with `$`. The value of the metadata is read from this path. In this case, the attribute refers to a list if it also ends with `[:]`. If it does not start with either `$` or `@`, it is used as the value of the metadata. The attribute `@target` configures the name of the metadata type. |
| `group` | Metadata groups are configured here. There are six attributes, which are explained below. Several `metadata` elements can be configured under a `group` element, whose `@source` attributes should start with `@`.  |
| `child` | Structure elements are configured here. There are five attributes, which are explained below. Several `metadata` elements can be configured under a `child` element, whose `@source` attributes should start with `@`. |


### Configuration of the attributes of a MetadataGroup
The following attributes can be configured in a `group` element:

| Value | Description |
| :--- | :--- |
| `@source` | JSON path to the parent element of a list. Starts with a `$`. |
| `@type` | Type of the metadata group. |
| `@altType` | Alternative type of the metadata group. If the alternative type is configured correctly, a metadata group of this type is created from all filtered entries. If the attribute is not configured or if the configured type cannot be found, all filtered entries are not imported. This parameter is optional. |
| `@key` | Key of the JSON objects for filtering. This parameter is optional. |
| `@value` | Value for comparison when filtering. This parameter is optional. |
| `@method` | Logic to be used for filtering. Options here are `is`, `not`, `startsWith`, `endsWith` and `contains`. This parameter is optional. The default value is `is`. |


### Configuration of the attributes of a DocStruct as a child
The following attributes can be configured in a `child` element:

| Value | Description |
| :--- | :--- |
| `@source` | JSON path to the parent element of a list. Starts with a `$`. |
| `@type` | Type of the structure element. |
| `@key` | Key of the JSON objects for filtering. This parameter is optional. |
| `@value` | Value for comparison when filtering. This parameter is optional. |
| `@method` | Logic to be used for filtering. Options here are `is`, `not`, `startsWith`, `endsWith` and `contains`. This parameter is optional. The DEFAULT value is `is`. |


### Configuration of the metadata for the partner's URL
There is an option to generate and save a metadata for the partner's URL. The URL to be generated is defined as follows: `{urlBase}/{urlPart_1}/{urlPart_2}/.../{urlPart_k}/{urlTail}/`

| Value | Description |
| :--- | :--- |
| `partnerUrl` | The `save` attribute configures whether a metadata should be generated for the partner's URL. The DEFAULT value is `false`. |
| `urlBase` | Here you configure the URL to the partner's server. This parameter is optional. A maximum of one hit is possible. |
| `urlPart` | The parts of the URL whose values from the JSON objects are to be appended to the `urlBase` one after the other are configured here. This parameter is optional. Multiple hits are possible. |
| `urlTail` | The trailing part of the URL is configured here. This parameter is optional. A maximum of one hit is possible. |
| `urlMetadata` | Type of metadata. |
