package de.intranda.goobi.plugins;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IPushPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.json.JSONArray;
import org.json.JSONObject;
import org.omnifaces.cdi.PushContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.ScriptThreadWithoutHibernate;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.StorageProviderInterface;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import net.xeoh.plugins.base.annotations.PluginImplementation;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataGroup;
import ugh.dl.MetadataGroupType;
import ugh.dl.MetadataType;
import ugh.dl.Person;
import ugh.dl.Prefs;
import ugh.exceptions.IncompletePersonObjectException;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.TypeNotAllowedAsChildException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.fileformats.mets.MetsMods;

@PluginImplementation
@Log4j2
public class ImportJsonWorkflowPlugin implements IWorkflowPlugin, IPushPlugin {

    private static final long serialVersionUID = 3915190112293742581L;
    @Getter
    private String title = "intranda_workflow_import_json";
    private long lastPush = System.currentTimeMillis();
    @Getter
    private transient List<ImportMetadata> importMetadata;
    private transient List<ImportGroupSet> importGroupSets;
    private transient List<ImportChildDocStruct> importChildDocStructs;
    private PushContext pusher;
    @Getter
    private boolean run = false;
    @Getter
    private int progress = -1;
    @Getter
    private int itemCurrent = 0;
    @Getter
    int itemsTotal = 0;
    @Getter
    private transient Queue<LogMessage> logQueue = new CircularFifoQueue<>(48);
    // folder used to hold the downloaded images temporarily
    private String importFolder;
    // workflow template that is to be used
    private String workflow;

    private String publicationType;
    // folder that contains the json files
    private String jsonFolder;
    // Set of names of the MetadataTypes that are configured to contain downloadable resources
    private Set<String> downloadableUrlSet = new HashSet<>();
    private String imageExtension = ".jpg";

    private HierarchicalConfiguration partnerUrlConfig;

    private transient StorageProviderInterface storageProvider = StorageProvider.getInstance();

    @Override
    public PluginType getType() {
        return PluginType.Workflow;
    }

    @Override
    public String getGui() {
        return "/uii/plugin_workflow_import_json.xhtml";
    }

    /**
     * Constructor
     */
    public ImportJsonWorkflowPlugin() {
        log.info("Sample importer workflow plugin started");

        // read important configuration first
        readConfiguration();
    }

    /**
     * private method to read main configuration file
     */
    private void readConfiguration() {
        updateLog("Start reading the configuration");

        XMLConfiguration config = ConfigPlugins.getPluginConfig(title);

        // read some main configuration
        importFolder = config.getString("importFolder");
        workflow = config.getString("workflow");
        publicationType = config.getString("publicationType");
        jsonFolder = config.getString("jsonFolder");
        String[] downloadableUrls = config.getStringArray("downloadableUrl");
        Collections.addAll(downloadableUrlSet, downloadableUrls);
        // configuration block for partner url
        partnerUrlConfig = config.configurationAt("partnerUrl");

        // read list of mapping configuration
        importMetadata = new ArrayList<>();
        List<HierarchicalConfiguration> mappings = config.configurationsAt("metadata");
        for (HierarchicalConfiguration node : mappings) {
            String source = node.getString("[@source]", "-");
            String target = node.getString("[@target]", "-");
            boolean person = node.getBoolean("[@person]", false);
            importMetadata.add(new ImportMetadata(source, target, person));
        }

        // initialize importGroupSets
        importGroupSets = new ArrayList<>();
        List<HierarchicalConfiguration> groupMappings = config.configurationsAt("group");
        for (HierarchicalConfiguration groupConfig : groupMappings) {
            String source = groupConfig.getString("[@source]", "-");
            String type = groupConfig.getString("[@type]", "-");
            String alternativeType = groupConfig.getString("[@altType]", "");
            String filteringKey = groupConfig.getString("[@key]", "");
            String filteringValue = groupConfig.getString("[@value]", "");
            String filteringMethod = groupConfig.getString("[@method]", "");
            ImportGroupSet groupSet = new ImportGroupSet(source, type, alternativeType, filteringKey, filteringValue, filteringMethod);
            // add elements to the group
            List<HierarchicalConfiguration> elementsMappings = groupConfig.configurationsAt("metadata");
            for (HierarchicalConfiguration node : elementsMappings) {
                String elementSource = node.getString("[@source]", "-");
                String elementTarget = node.getString("[@target]", "-");
                boolean person = node.getBoolean("[@person]", false);
                groupSet.addElement(new ImportMetadata(elementSource, elementTarget, person));
            }
            importGroupSets.add(groupSet);
        }

        // initialize importChildDocStructs
        importChildDocStructs = new ArrayList<>();
        List<HierarchicalConfiguration> childMappings = config.configurationsAt("child");
        for (HierarchicalConfiguration childConfig : childMappings) {
            String source = childConfig.getString("[@source]", "-");
            String type = childConfig.getString("[@type]", "-");
            String filteringKey = childConfig.getString("[@key]", "");
            String filteringValue = childConfig.getString("[@value]", "");
            String filteringMethod = childConfig.getString("[@method]", "");
            ImportChildDocStruct childStruct = new ImportChildDocStruct(source, type, filteringKey, filteringValue, filteringMethod);
            // add elements to the group
            List<HierarchicalConfiguration> elementsMappings = childConfig.configurationsAt("metadata");
            for (HierarchicalConfiguration node : elementsMappings) {
                String elementSource = node.getString("[@source]", "-");
                String elementTarget = node.getString("[@target]", "-");
                boolean person = node.getBoolean("[@person]", false);
                childStruct.addElement(new ImportMetadata(elementSource, elementTarget, person));
            }
            importChildDocStructs.add(childStruct);
        }

        // write a log into the UI
        updateLog("Configuration successfully read");
    }

    /**
     * cancel a running import
     */
    public void cancel() {
        run = false;
    }

    /**
     * main method to start the actual import
     */
    public void startImport() {
        progress = 0;
        BeanHelper bhelp = new BeanHelper();

        // run the import in a separate thread to allow a dynamic progress bar
        run = true;
        Runnable runnable = () -> {

            // read input file
            try {
                updateLog("Run through all import files");
                int start = 0;

                List<Path> jsonFiles = storageProvider.listFiles(jsonFolder);
                int end = jsonFiles.size();

                itemsTotal = end - start;
                itemCurrent = start;

                // run through import files (e.g. from importFolder)
                for (Path jsonFile : jsonFiles) {
                    Thread.sleep(100);
                    if (!run) {
                        break;
                    }

                    // create a unique process name
                    String processName = createProcessName();
                    updateLog("Start importing: " + processName, 1);

                    JSONObject jsonObject = getJsonObjectFromJsonFile(jsonFile);
                    if (jsonObject == null) {
                        log.debug("Failed to import from " + jsonFile);
                    } else {
                        // create and save the process
                        boolean success = tryCreateAndSaveNewProcess(bhelp, processName, jsonObject);
                        if (!success) {
                            String message = "Error while creating a process during the import";
                            reportError(message);
                        }

                        // recalculate progress
                        itemCurrent++;
                        progress = 100 * itemCurrent / itemsTotal;
                        updateLog("Processing of record done.");
                    }
                }

                // finally last push
                run = false;
                Thread.sleep(2000);
                updateLog("Import completed.");
            } catch (InterruptedException e) {
                Helper.setFehlerMeldung("Error while trying to execute the import: " + e.getMessage());
                log.error("Error while trying to execute the import", e);
                updateLog("Error while trying to execute the import: " + e.getMessage(), 3);
            }

        };
        new Thread(runnable).start();
    }

    @Override
    public void setPushContext(PushContext pusher) {
        this.pusher = pusher;
    }

    /**
     * create the title for the new process
     * 
     * @return new process title
     */
    private String createProcessName() {
        // create a process name via UUID
        String processName = UUID.randomUUID().toString();
        String regex = ConfigurationHelper.getInstance().getProcessTitleReplacementRegex();
        processName = processName.replaceAll(regex, "_").trim();

        // assure the uniqueness of the process name
        int tempCounter = 1;
        String tempName = processName;
        while (ProcessManager.countProcessTitle(processName, null) > 0) {
            processName = tempName + "_" + tempCounter;
            ++tempCounter;
        }

        return processName;
    }

    /**
     * get JSONObject of the input JSON file
     * 
     * @param jsonFile path to the json file
     * @return JSONObject
     */
    private JSONObject getJsonObjectFromJsonFile(Path jsonFile) {
        try (InputStream inputStream = storageProvider.newInputStream(jsonFile)) {
            // save the file's contents into a string
            String result = new String(IOUtils.toByteArray(inputStream));
            // create a JSONObject from this json string
            return new JSONObject(result);

        } catch (IOException e) {
            String message = "Errors happened while trying to create a JSONObject from contents of " + jsonFile;
            reportError(message);
            return null;
        }
    }

    /**
     * try to create and save a new process
     * 
     * @param bhelp BeanHelper
     * @param processName title of the new process
     * @param jsonObject JSONObject
     * @return true if a new process is successfully created and saved, otherwise false
     */
    private boolean tryCreateAndSaveNewProcess(BeanHelper bhelp, String processName, JSONObject jsonObject) {
        // get the correct workflow to use
        Process template = ProcessManager.getProcessByExactTitle(workflow);
        // prepare the Fileformat based on the template Process
        Fileformat fileformat = prepareFileformatForNewProcess(template, processName, jsonObject);
        if (fileformat == null) {
            // error happened during the preparation
            return false;
        }

        // save the process
        Process process = createAndSaveNewProcess(bhelp, template, processName, fileformat);
        if (process == null) {
            // error heppened while saving
            return false;
        }

        // copy files into the media folder of the process
        try {
            copyMediaFiles(process);
        } catch (IOException | SwapException | DAOException e) {
            String message = "Error while trying to copy files into the media folder: " + e.getMessage();
            reportError(message);
            return false;
        }

        // start open automatic tasks
        startOpenAutomaticTasks(process);

        updateLog("Process successfully created with ID: " + process.getId());

        return true;
    }

    /**
     * prepare the Fileformat for creating the new process
     * 
     * @param template Process template
     * @param processName title of the new process
     * @param jsonObject
     * @return Fileformat
     */
    private Fileformat prepareFileformatForNewProcess(Process template, String processName, JSONObject jsonObject) {
        Prefs prefs = template.getRegelsatz().getPreferences();
        try {
            Fileformat fileformat = new MetsMods(prefs);
            DigitalDocument dd = new DigitalDocument();
            fileformat.setDigitalDocument(dd);

            // add the physical basics
            DocStruct physical = dd.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
            dd.setPhysicalDocStruct(physical);
            Metadata mdForPath = new Metadata(prefs.getMetadataTypeByName("pathimagefiles"));
            mdForPath.setValue("file:///");
            physical.addMetadata(mdForPath);

            // add the logical basics
            DocStruct logical = dd.createDocStruct(prefs.getDocStrctTypeByName(publicationType));
            log.debug("logical has type = " + logical.getType());
            dd.setLogicalDocStruct(logical);

            // prepare source folder for this process to hold the downloaded images
            Path sourceFolder = Path.of(importFolder, processName);
            createFolder(sourceFolder);

            // create metadata fields
            createMetadataFields(prefs, logical, sourceFolder, jsonObject, this.importMetadata);

            // create metadata for partner url
            createMetadataPartnerUrl(prefs, logical, sourceFolder, jsonObject);

            // create MetadataGroups
            createMetadataGroups(prefs, logical, sourceFolder, jsonObject);

            // create children DocStructs
            createChildDocStructs(prefs, logical, sourceFolder, jsonObject, dd);

            return fileformat;

        } catch (PreferencesException | TypeNotAllowedForParentException | MetadataTypeNotAllowedException | IncompletePersonObjectException e) {
            String message = "Error while preparing the Fileformat for the new process: " + e.getMessage();
            reportError(message);
            return null;
        }
    }

    private void createFolder(Path sourceFolder) {
        try {
            storageProvider.createDirectories(sourceFolder);
        } catch (IOException e) {
            String message = "failed to create directories: " + sourceFolder;
            reportError(message);
        }
    }

    /**
     * create and save the new process
     * 
     * @param bhelp BeanHelper
     * @param template Process template
     * @param processName title of the new process
     * @param fileformat Fileformat
     * @return the new process created
     */
    private Process createAndSaveNewProcess(BeanHelper bhelp, Process template, String processName, Fileformat fileformat) {
        // save the process
        Process process = bhelp.createAndSaveNewProcess(template, processName, fileformat);

        // add some properties
        bhelp.EigenschaftHinzufuegen(process, "Template", template.getTitel());
        bhelp.EigenschaftHinzufuegen(process, "TemplateID", "" + template.getId());

        try {
            ProcessManager.saveProcess(process);
        } catch (DAOException e) {
            String message = "Error while trying to save the process: " + e.getMessage();
            reportError(message);
            return null;
        }

        return process;
    }

    /**
     * copy the images from importFolder to media folders of the process
     * 
     * @param process Process whose media folder is targeted
     * @throws IOException
     * @throws SwapException
     * @throws DAOException
     */
    private void copyMediaFiles(Process process) throws IOException, SwapException, DAOException {
        // if media files are given, import these into the media folder of the process
        updateLog("Start copying media files");
        // prepare the directories
        String mediaBase = process.getImagesTifDirectory(false);
        storageProvider.createDirectories(Path.of(mediaBase));
        String targetFolder = Path.of(importFolder, process.getTitel()).toString();
        List<Path> filesToImport = storageProvider.listFiles(targetFolder);
        for (Path path : filesToImport) {
            File file = path.toFile();
            if (file.canRead()) {
                String fileName = path.getFileName().toString();
                log.debug("fileName = " + fileName);
                Path targetPath = Path.of(mediaBase, fileName);
                storageProvider.move(path, targetPath);
            }
        }
        storageProvider.deleteDir(Path.of(targetFolder));
    }

    /**
     * start all automatic tasks that are open
     * 
     * @param process
     */
    private void startOpenAutomaticTasks(Process process) {
        // start any open automatic tasks for the created process
        for (Step s : process.getSchritteList()) {
            if (StepStatus.OPEN.equals(s.getBearbeitungsstatusEnum()) && s.isTypAutomatisch()) {
                ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                myThread.startOrPutToQueue();
            }
        }
    }

    /**
     * create all metadata fields
     * 
     * @param prefs Prefs
     * @param ds DocStruct
     * @param sourceFolder path to the folder that is used to hold the downloaded images
     * @param jsonObject
     * @param importSets list of ImportMetadata
     */
    private void createMetadataFields(Prefs prefs, DocStruct ds, Path sourceFolder, JSONObject jsonObject, List<ImportMetadata> importSets) {
        for (ImportMetadata importSet : importSets) {
            // retrieve the value from the configured jsonPath
            String source = importSet.getSource();
            List<String> values = getValuesFromSource(source, jsonObject);

            // prepare the MetadataType
            String target = importSet.getTarget();
            MetadataType targetType = prefs.getMetadataTypeByName(target);
            log.debug("targetType = " + targetType);
            boolean isDownloadableUrl = downloadableUrlSet.contains(target);

            boolean isPerson = importSet.isPerson();

            for (String value : values) {
                try {
                    Metadata md = createMetadata(targetType, value, isPerson, isDownloadableUrl, sourceFolder);
                    if (isPerson) {
                        updateLog("Add person '" + target + "' with value '" + value + "'");
                        ds.addPerson((Person) md);
                    } else {
                        updateLog("Add metadata '" + target + "' with value '" + value + "'");
                        log.debug("ds.type = " + ds.getType());
                        ds.addMetadata(md);
                    }
                } catch (MetadataTypeNotAllowedException e) {
                    String message = "MetadataType " + target + " is not allowed. Skipping...";
                    reportError(message);
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * create the metadata specifically for the configured partner url
     * 
     * @param prefs Prefs
     * @param ds DocStruct
     * @param sourceFolder path to the folder that is used to hold the downloaded images
     * @param jsonObject
     */
    private void createMetadataPartnerUrl(Prefs prefs, DocStruct ds, Path sourceFolder, JSONObject jsonObject) {
        String partnerUrl = getPartnerUrl(partnerUrlConfig, jsonObject);
        log.debug("partnerUrl = " + partnerUrl);
        String partnerUrlType = partnerUrlConfig.getString("urlMetadata");
        log.debug("partnerUrlType = " + partnerUrlType);
        if (StringUtils.isAnyBlank(partnerUrl, partnerUrlType)) {
            // no valid partner url or no specified metadata
            return;
        }
        MetadataType urlType = prefs.getMetadataTypeByName(partnerUrlType);
        try {
            // we don't want to download from this url, hence the second false
            Metadata md = createMetadata(urlType, partnerUrl, false, false, sourceFolder);
            updateLog("Add metadata '" + partnerUrlType + "' with value '" + partnerUrl + "'");
            ds.addMetadata(md);
        } catch (MetadataTypeNotAllowedException e) {
            String message = "MetadataType " + partnerUrlType + " is not allowed. Skipping...";
            reportError(message);
        }
    }

    /**
     * get the value of the partner url
     * 
     * @param partnerUrlConfig
     * @param jsonObject
     * @return the value of the partner url as a string
     */
    private String getPartnerUrl(HierarchicalConfiguration partnerUrlConfig, JSONObject jsonObject) {
        boolean shouldSave = partnerUrlConfig.getBoolean("[@save]");
        if (!shouldSave) {
            return "";
        }
        // otherwise create the string
        String urlBase = partnerUrlConfig.getString("urlBase", "");
        String[] urlParts = partnerUrlConfig.getStringArray("urlPart");
        String urlTail = partnerUrlConfig.getString("urlTail", "");
        return createPartnerUrl(urlBase, urlParts, urlTail, jsonObject);
    }

    /**
     * create the value of the partner url based on the url base, several parts and a tail
     * 
     * @param urlBase starting part of the url
     * @param urlParts multiple parts of the url following base
     * @param urlTail ending part of the url
     * @param jsonObject
     * @return {urlBase}/{urlPart_1}/{urlPart_2}/.../{urlTail}
     */
    private String createPartnerUrl(String urlBase, String[] urlParts, String urlTail, JSONObject jsonObject) {
        log.debug("starting to create partner url");
        StringBuilder sb = new StringBuilder(urlBase);
        if (!urlBase.endsWith("/")) {
            sb.append("/");
        }
        for (String urlPart : urlParts) {
            List<String> values = getValuesFromSource(urlPart, jsonObject);
            for (String value : values) {
                sb.append(value);
                if (!value.endsWith("/")) {
                    sb.append("/");
                }
            }
        }

        if (StringUtils.isNotBlank(urlTail)) {
            sb.append(urlTail);
            if (!urlTail.endsWith("/")) {
                sb.append("/");
            }
        }

        return sb.toString();
    }

    /**
     * create Metadata
     * 
     * @param targetType MetadataType
     * @param value value of the new Metadata
     * @param isPerson
     * @param isDownloadableUrl
     * @param sourceFolder path to the folder that is used to hold the downloaded images
     * @return the new Metadata object created
     * @throws MetadataTypeNotAllowedException
     */
    private Metadata createMetadata(MetadataType targetType, String value, boolean isPerson, boolean isDownloadableUrl, Path sourceFolder)
            throws MetadataTypeNotAllowedException {
        // treat persons different than regular metadata
        if (isPerson) {
            Person p = new Person(targetType);
            int splitIndex = value.indexOf(" ");
            String firstName = value.substring(0, splitIndex);
            String lastName = value.substring(splitIndex);
            p.setFirstname(firstName);
            p.setLastname(lastName);

            return p;
        }

        Metadata md = new Metadata(targetType);
        md.setValue(value);

        if (isDownloadableUrl) {
            // download the image from the url to the sourceFolder
            downloadImage(value, sourceFolder.toString());
        }
        return md;
    }

    /**
     * download the image from the given url
     * 
     * @param strUrl url of the image
     * @param targetFolder targeted folder to download the image file
     */
    private void downloadImage(String strUrl, String targetFolder) {
        log.debug("downloading image from url: " + strUrl);
        // check url
        URL url = null;
        try {
            url = new URL(strUrl);
        } catch (MalformedURLException e) {
            String message = "the input URL is malformed: " + strUrl;
            reportError(message);
            return;
        }

        // url is correctly formed, start to download
        String imageName = getImageNameFromUrl(url);
        Path targetPath = Path.of(targetFolder, imageName);

        try (ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
                FileOutputStream outputStream = new FileOutputStream(targetPath.toString())) {

            FileChannel fileChannel = outputStream.getChannel();
            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);

        } catch (IOException e) {
            String message = "failed to download the image from " + strUrl;
            reportError(message);
        }
    }

    /**
     * get the image name from a URL object
     * 
     * @param url the URL object
     * @return the full image name including the configured image extension
     */
    private String getImageNameFromUrl(URL url) {
        String urlFileName = url.getFile();
        log.debug("urlFileName = " + urlFileName);

        String imageName = urlFileName.replaceAll("\\W", "_") + imageExtension;
        log.debug("imageName = " + imageName);

        return imageName;
    }

    /**
     * create MetadataGroups
     * 
     * @param prefs Prefs
     * @param ds DocStruct
     * @param sourceFolder path to the folder that is used to hold the downloaded images
     * @param jsonObject
     * @throws MetadataTypeNotAllowedException
     */
    private void createMetadataGroups(Prefs prefs, DocStruct ds, Path sourceFolder, JSONObject jsonObject) throws MetadataTypeNotAllowedException {
        log.debug("creating metadata groups");
        for (ImportGroupSet group : importGroupSets) {
            String groupSource = group.getSource();
            String type = group.getType();
            log.debug("group.source = " + groupSource);
            log.debug("group.type = " + type);
            List<ImportMetadata> elements = group.getElements();
            log.debug("group has " + elements.size() + " elements");

            // items needed by the filtering logic
            String alternativeType = group.getAlternativeType();
            String filteringKey = group.getFilteringKey();
            String filteringValue = group.getFilteringValue();
            String filteringMethod = group.getFilteringMethod();

            //prepare metadata group type and an alternative type
            MetadataGroupType groupType = prefs.getMetadataGroupTypeByName(type);
            MetadataGroupType alternativeGroupType = getAlternativeMetadataGroupType(prefs, alternativeType);

            JSONObject tempObject = getDirectParentOfLeafObject(groupSource, jsonObject);
            String arrayName = groupSource.substring(groupSource.lastIndexOf(".") + 1);
            log.debug("arrayName = " + arrayName);
            // check existence of key
            if (!tempObject.has(arrayName)) {
                return;
            }
            JSONArray elementsArray = tempObject.getJSONArray(arrayName);
            // every JSONObject of this JSONArray should become a metadata group
            for (int k = 0; k < elementsArray.length(); ++k) {
                JSONObject elementObject = elementsArray.getJSONObject(k);
                // check elementObject to see whether we should use the alternative group type
                boolean useOriginalType = isFilteringLogicPassed(elementObject, filteringKey, filteringValue, filteringMethod);
                MetadataGroup mdGroup = useOriginalType ? createMetadataGroup(groupType) : createMetadataGroup(alternativeGroupType);
                if (mdGroup == null) {
                    continue;
                }

                for (ImportMetadata element : elements) {
                    // every element should be a metadata in this metadata group
                    log.debug(element);
                    String elementSource = element.getSource();
                    String elementTypeName = element.getTarget();
                    log.debug("elementSource = " + elementSource);
                    log.debug("elementTypeName = " + elementTypeName);
                    MetadataType elementType = prefs.getMetadataTypeByName(elementTypeName);
                    List<String> values = getValuesFromJsonObject(elementSource, elementObject);
                    for (String value : values) {
                        // for every value create a Metadata of that value
                        log.debug("value = " + value);
                        Metadata md = createMetadata(elementType, value, false, false, sourceFolder);
                        mdGroup.addMetadata(md);
                    }
                }
                ds.addMetadataGroup(mdGroup);
            }
        }
    }

    /**
     * get the alternative MetadataGroupType
     * 
     * @param prefs Prefs
     * @param alternativeType name of the alternative MetadataGroupType
     * @return the MetadataGroupType if it is found, otherwise null
     */
    private MetadataGroupType getAlternativeMetadataGroupType(Prefs prefs, String alternativeType) {
        MetadataGroupType type = null;
        try {
            type = prefs.getMetadataGroupTypeByName(alternativeType);
        } catch (Exception e) {
            // no need to do anything
        }

        return type;
    }

    /**
     * create a MetadataGroup based on the input MetadataGroupType
     * 
     * @param type MetadataGroupType
     * @return a new MetadataGroup if it is successfully created, or null otherwise
     */
    private MetadataGroup createMetadataGroup(MetadataGroupType type) {
        try {
            return new MetadataGroup(type);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * check whether the filtering logic configured for MetadataGroups and children DocStructs could pass
     * 
     * @param jsonObject
     * @param filteringKey key of the JSONObject's item that should be checked
     * @param filteringValue value that should be compared
     * @param filteringMethod how should the values be compared
     * @return true if the filtering logic could pass, i.e. the value satisfies the expectation, otherwise false
     */
    private boolean isFilteringLogicPassed(JSONObject jsonObject, String filteringKey, String filteringValue, String filteringMethod) {
        if (StringUtils.isBlank(filteringKey)) {
            return true;
        }
        String value = String.valueOf(jsonObject.get(filteringKey));
        switch (filteringMethod) {
            case "is":
                return value.equals(filteringValue);
            case "not":
                return !value.equals(filteringValue);
            case "startsWith":
                return value.startsWith(filteringValue);
            case "endsWith":
                return value.endsWith(filteringValue);
            case "contains":
                return value.contains(filteringValue);
            default:
                // unrecognized method
                return true;
        }
    }

    /**
     * create all children DocStructs
     * 
     * @param prefs Prefs
     * @param ds DocStruct
     * @param sourceFolder path to the folder that is used to hold the downloaded images
     * @param jsonObject
     * @param dd DigitalDocument
     */
    private void createChildDocStructs(Prefs prefs, DocStruct ds, Path sourceFolder, JSONObject jsonObject, DigitalDocument dd) {
        log.debug("creating children DocStructs");
        log.debug("importChildDocStructs has length = " + importChildDocStructs.size());
        for (ImportChildDocStruct struct : importChildDocStructs) {
            String structSource = struct.getSource();
            String structType = struct.getType();
            log.debug("structSource = " + structSource);
            log.debug("structType = " + structType);

            // items needed by the filtering logic
            String filteringKey = struct.getFilteringKey();
            String filteringValue = struct.getFilteringValue();
            String filteringMethod = struct.getFilteringMethod();

            List<ImportMetadata> childImportMetadata = struct.getElements();

            JSONObject tempObject = getDirectParentOfLeafObject(structSource, jsonObject);
            String arrayName = structSource.substring(structSource.lastIndexOf(".") + 1);
            log.debug("arrayName = " + arrayName);
            // check existence of key
            if (!tempObject.has(arrayName)) {
                return;
            }
            JSONArray elementsArray = tempObject.getJSONArray(arrayName);
            // process every JSONObject in this JSONArray
            for (int k = 0; k < elementsArray.length(); ++k) {
                // every sub-element should be a child DocStruct
                JSONObject elementObject = elementsArray.getJSONObject(k);
                boolean addThisStruct = isFilteringLogicPassed(elementObject, filteringKey, filteringValue, filteringMethod);
                if (!addThisStruct) {
                    String message = "The configured filtering logic did not pass, child No." + (k + 1) + " will not be created.";
                    updateLog(message);
                    continue;
                }
                try {
                    DocStruct childStruct = dd.createDocStruct(prefs.getDocStrctTypeByName(structType));
                    log.debug("childStruct has type = " + childStruct.getType());
                    ds.addChild(childStruct);
                    // create metadata fields to the child DocStruct
                    createMetadataFields(prefs, childStruct, sourceFolder, elementObject, childImportMetadata);
                } catch (TypeNotAllowedForParentException | TypeNotAllowedAsChildException e) {
                    String message = "Error while trying to create children DocStructs";
                    reportError(message);
                }
            }
        }
    }

    /**
     * get values from the configured json path
     * 
     * @param source configured json path
     * @param jsonObject
     * @return list of string values
     */
    private List<String> getValuesFromSource(String source, JSONObject jsonObject) {
        List<String> results = new ArrayList<>();
        // for source paths indicating jsonPaths, it should start with $. or with @.
        if (!source.startsWith("$") && !source.startsWith("@")) {
            // for those that are not json paths, just return themselves trimmed
            results.add(source.trim());
            return results;
        }

        // get to the JSONObject
        JSONObject tempObject = getDirectParentOfLeafObject(source, jsonObject);
        if (tempObject == null) {
            return results;
        }
        // the key is the tailing part of source after the last dot
        String key = source.substring(source.lastIndexOf(".") + 1);

        return getValuesFromJsonObject(key, tempObject);
    }

    /**
     * get to the direct parent of the leaf object
     * 
     * @param source json path
     * @param jsonObject
     * @return the direct parent of the leaf object as JSONObject
     */
    private JSONObject getDirectParentOfLeafObject(String source, JSONObject jsonObject) {
        String[] paths = source.split("\\.");
        JSONObject tempObject = jsonObject;
        // skip the first one which is nothing but the heading $
        for (int i = 1; i < paths.length - 1; ++i) {
            log.debug("moving forward to " + paths[i]);
            // check existence of the key
            if (!tempObject.has(paths[i])) {
                return null;
            }
            tempObject = tempObject.getJSONObject(paths[i]);
        }

        return tempObject;
    }

    /**
     * get values from the JSONObject
     * 
     * @param key
     * @param jsonObject
     * @return list of string values
     */
    private List<String> getValuesFromJsonObject(String key, JSONObject jsonObject) {
        // suppose from now on our jsonObject is the one that is the direct parent of some leaf nodes
        // i.e. source will not start with $
        // if it is not starting with @ then it is just a value of the jsonObject
        // otherwise it is a child importSet
        List<String> results = new ArrayList<>();
        String filteredKey = key.startsWith("@") ? key.substring(2) : key;

        // check existence of the key
        if (!jsonObject.has(filteredKey) && !filteredKey.endsWith("[:]")) {
            return results;
        }

        if (!filteredKey.endsWith("[:]")) {
            // it is not an array
            String result = String.valueOf(jsonObject.get(filteredKey));
            // filter out null values
            if (StringUtils.isNotBlank(result) && !"null".equalsIgnoreCase(result)) {
                results.add(result);
            }
            return results;
        }

        // it is an array
        // remove the tailing [:]
        log.debug("tailing [:] encountered");
        String arrayName = filteredKey.substring(0, filteredKey.length() - 3);
        log.debug("arrayName = " + arrayName);
        // check existence of the key again for array
        if (!jsonObject.has(arrayName)) {
            return results;
        }
        JSONArray jsonArray = jsonObject.getJSONArray(arrayName);
        log.debug("jsonArray has length = " + jsonArray.length());
        for (int i = 0; i < jsonArray.length(); ++i) {
            String value = jsonArray.getString(i);
            log.debug("value in jsonArray = " + value);
            results.add(value);
        }

        return results;
    }

    /**
     * report error
     * 
     * @param message error message
     */
    private void reportError(String message) {
        log.error(message);
        updateLog(message, 3);
        Helper.setFehlerMeldung(message);
        pusher.send("error");
    }

    /**
     * simple method to send status message to gui
     * 
     * @param logmessage
     */
    private void updateLog(String logmessage) {
        updateLog(logmessage, 0);
    }

    /**
     * simple method to send status message with specific level to gui
     * 
     * @param logmessage
     */
    private void updateLog(String logmessage, int level) {
        logQueue.add(new LogMessage(logmessage, level));
        log.debug(logmessage);
        if (pusher != null && System.currentTimeMillis() - lastPush > 500) {
            lastPush = System.currentTimeMillis();
            pusher.send("update");
        }
    }

    @Data
    @AllArgsConstructor
    public class ImportMetadata {
        private String source;
        private String target;
        private boolean person;

        @Override
        public String toString() {
            return source + " -> " + target;
        }
    }

    @Data
    public class ImportGroupSet {
        private String source;
        private String type;
        private String alternativeType = "";
        private String filteringKey = "";
        private String filteringValue = "";
        private String filteringMethod = "is";
        private List<ImportMetadata> elements;

        public ImportGroupSet(String source, String type) {
            this.source = source;
            this.type = type;
            this.elements = new ArrayList<>();
        }

        public ImportGroupSet(String source, String type, String altType, String key, String value, String method) {
            this.source = source;
            this.type = type;
            this.alternativeType = altType;
            this.filteringKey = key;
            this.filteringValue = value;
            switch (method) {
                case "not":
                    this.filteringMethod = "not";
                    break;
                case "startsWith":
                    this.filteringMethod = "startsWith";
                    break;
                case "endsWith":
                    this.filteringMethod = "endsWith";
                    break;
                case "contains":
                    this.filteringMethod = "contains";
                    break;
                default:
                    // otherwise do nothing, use default method "is"
            }
            this.elements = new ArrayList<>();
        }

        public void addElement(ImportMetadata element) {
            elements.add(element);
        }
    }

    @Data
    public class ImportChildDocStruct {
        private String source;
        private String type;
        private String filteringKey = "";
        private String filteringValue = "";
        private String filteringMethod = "is";
        private List<ImportMetadata> elements;

        public ImportChildDocStruct(String source, String type) {
            this.source = source;
            this.type = type;
            this.elements = new ArrayList<>();
        }

        public ImportChildDocStruct(String source, String type, String key, String value, String method) {
            this.source = source;
            this.type = type;
            this.filteringKey = key;
            this.filteringValue = value;
            switch (method) {
                case "not":
                    this.filteringMethod = "not";
                    break;
                case "startsWith":
                    this.filteringMethod = "startsWith";
                    break;
                case "endsWith":
                    this.filteringMethod = "endsWith";
                    break;
                case "contains":
                    this.filteringMethod = "contains";
                    break;
                default:
                    // otherwise do nothing, use default method "is"
            }
            this.elements = new ArrayList<>();
        }

        public void addElement(ImportMetadata element) {
            elements.add(element);
        }
    }

    @Data
    @AllArgsConstructor
    public class LogMessage {
        private String message;
        private int level = 0;
    }
}
