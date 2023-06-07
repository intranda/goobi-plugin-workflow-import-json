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
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
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

    @Getter
    private String title = "intranda_workflow_import_json";
    private long lastPush = System.currentTimeMillis();
    @Getter
    private List<ImportSet> importSets;
    private List<ImportGroupSet> importGroupSets;
    private List<ImportChildDocStruct> importChildDocStructs;
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
    private Queue<LogMessage> logQueue = new CircularFifoQueue<>(48);
    private String importFolder;
    private String workflow;
    private String publicationType;
    
    private String jsonFolder;

    private String urlMetadata;
    private String imageExtension = ".jpg";

    private StorageProviderInterface storageProvider = StorageProvider.getInstance();

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
        urlMetadata = config.getString("urlMetadata", "");
        
        // read list of mapping configuration
        importSets = new ArrayList<>();
        List<HierarchicalConfiguration> mappings = config.configurationsAt("importSet");
        for (HierarchicalConfiguration node : mappings) {
            String settitle = node.getString("[@title]", "-");
            String source = node.getString("[@source]", "-");
            String target = node.getString("[@target]", "-");
            boolean person = node.getBoolean("[@person]", false);
            importSets.add(new ImportSet(settitle, source, target, person));
        }
        
        // initialize importGroupSets
        importGroupSets = new ArrayList<>();
        List<HierarchicalConfiguration> groupMappings = config.configurationsAt("group");
        for (HierarchicalConfiguration groupConfig : groupMappings) {
            String source = groupConfig.getString("[@source]", "-");
            String type = groupConfig.getString("[@type]", "-");
            ImportGroupSet groupSet = new ImportGroupSet(source, type);
            // add elements to the group
            List<HierarchicalConfiguration> elementsMappings = groupConfig.configurationsAt("importSet");
            for (HierarchicalConfiguration node : elementsMappings) {
                String elementTitle = node.getString("[@title]", "-");
                String elementSource = node.getString("[@source]", "-");
                String elementTarget = node.getString("[@target]", "-");
                boolean person = node.getBoolean("[@person]", false);
                groupSet.addElement(new ImportSet(elementTitle, elementSource, elementTarget, person));
            }
            importGroupSets.add(groupSet);
        }

        // initialize importChildDocStructs
        importChildDocStructs = new ArrayList<>();
        List<HierarchicalConfiguration> childMappings = config.configurationsAt("child");
        for (HierarchicalConfiguration childConfig : childMappings) {
            String source = childConfig.getString("[@source]", "-");
            String type = childConfig.getString("[@type]", "-");
            ImportChildDocStruct childStruct = new ImportChildDocStruct(source, type);
            // add elements to the group
            List<HierarchicalConfiguration> elementsMappings = childConfig.configurationsAt("importSet");
            for (HierarchicalConfiguration node : elementsMappings) {
                String elementTitle = node.getString("[@title]", "-");
                String elementSource = node.getString("[@source]", "-");
                String elementTarget = node.getString("[@target]", "-");
                boolean person = node.getBoolean("[@person]", false);
                childStruct.addElement(new ImportSet(elementTitle, elementSource, elementTarget, person));
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
     * 
     * @param importConfiguration
     */
    public void startImport(ImportSet importset) {
    	updateLog("Start import for: " + importset.getTitle());
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

                    JSONObject jsonObject = getJsonObjectFromPath(jsonFile);
                    if (jsonObject == null) {
                        log.debug("jsonObject is null");
                        return;
                    }

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
	 * simple method to send status message to gui
	 * @param logmessage
	 */
	private void updateLog(String logmessage) {
		updateLog(logmessage, 0);
	}
	
	/**
	 * simple method to send status message with specific level to gui
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
	
    private String createProcessName() {
        // create a process name via UUID
        String processName = UUID.randomUUID().toString();
        String regex = ConfigurationHelper.getInstance().getProcessTitleReplacementRegex();
        processName = processName.replaceAll(regex, "_").trim();

        // assure the uniqueness of the process name
        if (ProcessManager.countProcessTitle(processName, null) > 0) {
            int tempCounter = 1;
            String tempName = processName + "_" + tempCounter;
            while (ProcessManager.countProcessTitle(tempName, null) > 0) {
                tempCounter++;
                tempName = processName + "_" + tempCounter;
            }
            processName = tempName;
        }

        return processName;
    }
    
    private JSONObject getJsonObjectFromPath(Path jsonFile) {
        try (InputStream inputStream = storageProvider.newInputStream(jsonFile)) {
            // save the file's contents into a string
            String result = new String(IOUtils.toByteArray(inputStream));
            //            log.debug(result);
            // create a JSONObject from this json string
            JSONObject jsonObject = new JSONObject(result);

            return jsonObject;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    private boolean tryCreateAndSaveNewProcess(BeanHelper bhelp, String processName, JSONObject jsonObject) {
        // get the correct workflow to use
        Process template = ProcessManager.getProcessByExactTitle(workflow);
        // prepare the Fileformat based on the template Process
        Fileformat fileformat = prepareFileformatForNewProcess(template, jsonObject);
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

    private Fileformat prepareFileformatForNewProcess(Process template, JSONObject jsonObject) {
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
            dd.setLogicalDocStruct(logical);

            // create the metadata fields by reading the config (and get content from the content files of course)
            createMetadataFields(prefs, logical, jsonObject, this.importSets);

            // create MetadataGroups
            //            createMetadataGroups(prefs, logical, jsonObject);

            // create child DocStructs
            createChildDocStructs(prefs, dd, logical, jsonObject);

            return fileformat;

        } catch (PreferencesException | TypeNotAllowedForParentException | MetadataTypeNotAllowedException | IncompletePersonObjectException
                | TypeNotAllowedAsChildException e) {
            String message = "Error while preparing the Fileformat for the new process: " + e.getMessage();
            reportError(message);
            return null;
        }
    }

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

    private void copyMediaFiles(Process process) throws IOException, SwapException, DAOException {
        // if media files are given, import these into the media folder of the process
        updateLog("Start copying media files");
        String targetBase = process.getImagesOrigDirectory(false);
        // prepare the directories
        storageProvider.createDirectories(Path.of(targetBase));
        List<Path> filesToImport = storageProvider.listFiles(importFolder);
        for (Path path : filesToImport) {
            File file = path.toFile();
            if (file.canRead()) {
                String fileName = path.getFileName().toString();
                log.debug("fileName = " + fileName);
                Path targetPath = Path.of(targetBase, fileName);
                storageProvider.copyFile(path, targetPath);
            }
        }
    }

    private void startOpenAutomaticTasks(Process process) {
        // start any open automatic tasks for the created process
        for (Step s : process.getSchritteList()) {
            if (s.getBearbeitungsstatusEnum().equals(StepStatus.OPEN) && s.isTypAutomatisch()) {
                ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                myThread.startOrPutToQueue();
            }
        }
    }

    private void createMetadataFields(Prefs prefs, DocStruct ds, JSONObject jsonObject, List<ImportSet> importSets)
            throws MetadataTypeNotAllowedException {
        for (ImportSet importSet : importSets) {
            // retrieve the value from the configured jsonPath
            String source = importSet.getSource();
            List<String> values = getValuesFromEasySource(source, jsonObject);
            // prepare the MetadataType
            String target = importSet.getTarget();
            MetadataType targetType = prefs.getMetadataTypeByName(target);
            boolean isUrl = urlMetadata.equals(target);

            boolean isPerson = importSet.isPerson();

            for (String value : values) {
                Metadata md = createMetadata(targetType, value, isPerson, isUrl);
                if (isPerson) {
                    updateLog("Add person '" + target + "' with value '" + value + "'");
                    ds.addPerson((Person) md);
                } else {
                    updateLog("Add metadata '" + target + "' with value '" + value + "'");
                    ds.addMetadata(md);
                }
            }
        }
    }

    private Metadata createMetadata(MetadataType targetType, String value, boolean isPerson, boolean isUrl) throws MetadataTypeNotAllowedException {
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
        if (isUrl) {
            // download the image from the url to the importFolder
            downloadImageFile(value, importFolder);
        }

        return md;
    }

    /**
     * download the image file from the given url
     * 
     * @param strUrl url of the image file
     * @param processImageFolder media folder of the process
     * @return the image file as a File object if it is successfully downloaded, null if any IOException should occur
     */
    private File downloadImageFile(String strUrl, String processImageFolder) {
        log.debug("downloading image from url: " + strUrl);
        // check url
        URL url = null;
        try {
            url = new URL(strUrl);
        } catch (MalformedURLException e) {
            String message = "the input URL is malformed: " + strUrl;
            reportError(message);
            return null;
        }

        // url is correctly formed, start to download
        String imageName = getImageNameFromUrl(url);
        Path targetPath = Path.of(processImageFolder, imageName);

        try (ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
                FileOutputStream outputStream = new FileOutputStream(targetPath.toString())) {

            FileChannel fileChannel = outputStream.getChannel();
            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            return new File(targetPath.toString());

        } catch (IOException e) {
            String message = "failed to download the image from " + strUrl;
            reportError(message);
            return null;
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

    private void createMetadataGroups(Prefs prefs, DocStruct ds, JSONObject jsonObject) throws MetadataTypeNotAllowedException {
        log.debug("creating metadata groups");
        for (ImportGroupSet group : importGroupSets) {
            String groupSource = group.getSource();
            String type = group.getType();
            log.debug("group.source = " + groupSource);
            log.debug("group.type = " + type);
            List<ImportSet> elements = group.getElements();
            log.debug("group has " + elements.size() + " elements");

            // create and add the MetadataGroup
            //            MetadataGroupType groupType = prefs.getMetadataGroupTypeByName(type);
            //            MetadataGroup mdGroup = new MetadataGroup(groupType);
            //            ds.addMetadataGroup(mdGroup);

            JSONObject tempObject = getDirectParentOfLeafObject(groupSource, jsonObject);
            String arrayName = groupSource.substring(groupSource.lastIndexOf(".") + 1);
            log.debug("arrayName = " + arrayName);
            JSONArray elementsArray = tempObject.getJSONArray(arrayName);
            // process every JSONObject in this JSONArray
            for (int k = 0; k < elementsArray.length(); ++k) {
                JSONObject elementObject = elementsArray.getJSONObject(k);
                // every sub-element should be a DocStruct
                for (ImportSet element : elements) {
                    log.debug(element);
                    String elementSource = element.getSource();
                    String elementType = element.getTarget();
                    log.debug("elementSource = " + elementSource);
                    log.debug("elementType = " + elementType);
                    List<String> values = getValuesFromJsonObject(elementSource, elementObject);
                    for (String value : values) {
                        // for every value create a Metadata of that value
                        log.debug("value = " + value);
                    }
                }
            }
        }
    }

    private void createChildDocStructs(Prefs prefs, DigitalDocument dd, DocStruct ds, JSONObject jsonObject)
            throws TypeNotAllowedForParentException, TypeNotAllowedAsChildException, MetadataTypeNotAllowedException {
        log.debug("creating child DocStructs");
        log.debug("importChildDocStructs has length = " + importChildDocStructs.size());
        for (ImportChildDocStruct struct : importChildDocStructs) {
            String structSource = struct.getSource();
            String structType = struct.getType();
            log.debug("structSource = " + structSource);
            log.debug("structType = " + structType);

            List<ImportSet> childImportSets = struct.getElements();
            
            JSONObject tempObject = getDirectParentOfLeafObject(structSource, jsonObject);
            String arrayName = structSource.substring(structSource.lastIndexOf(".") + 1);
            log.debug("arrayName = " + arrayName);
            JSONArray elementsArray = tempObject.getJSONArray(arrayName);
            // process every JSONObject in this JSONArray
            for (int k = 0; k < elementsArray.length(); ++k) {
                // every sub-element should be a child DocStruct
                JSONObject elementObject = elementsArray.getJSONObject(k);
                DocStruct childStruct = dd.createDocStruct(prefs.getDocStrctTypeByName(structType));
                ds.addChild(childStruct);
                // create metadata fields to the child DocStruct
                createMetadataFields(prefs, childStruct, elementObject, childImportSets);
            }
        }
    }

    private List<String> getValuesFromEasySource(String source, JSONObject jsonObject) {
        List<String> results = new ArrayList<>();
        // for source paths indicating jsonPaths, it should start with $. or with @.
        if (!source.startsWith("$") && !source.startsWith("@")) {
            // for those that are not json paths, just return themselves trimmed
            results.add(source.trim());
            return results;
        }

        // get to the JSONObject
        JSONObject tempObject = getDirectParentOfLeafObject(source, jsonObject);
        // the key is the tailing part of source after the last dot
        String key = source.substring(source.lastIndexOf(".") + 1);
        
        return getValuesFromJsonObject(key, tempObject);
    }

    private JSONObject getDirectParentOfLeafObject(String source, JSONObject jsonObject) {
        String[] paths = source.split("\\.");
        JSONObject tempObject = jsonObject;
        // skip the first one which is nothing but the heading $
        for (int i = 1; i < paths.length - 1; ++i) {
            log.debug("moving forward to " + paths[i]);
            tempObject = tempObject.getJSONObject(paths[i]);
        }

        return tempObject;
    }

    private List<String> getValuesFromJsonObject(String key, JSONObject jsonObject) {
        // suppose from now on our jsonObject is the one that is the direct parent of some leaf nodes
        // i.e. source will not start with $
        // if it is not starting with @ then it is just a value of the jsonObject
        // otherwise it is a child importSet
        List<String> results = new ArrayList<>();
        String filteredKey = key.startsWith("@") ? key.substring(2) : key;

        if (!filteredKey.endsWith("[:]")) {
            // it is not an array
            results.add(jsonObject.getString(filteredKey));
            return results;
        }

        // it is an array
        // remove the tailing [:]
        String arrayName = filteredKey.substring(0, key.length() - 3);
        JSONArray jsonArray = jsonObject.getJSONArray(arrayName);
        for (int i = 0; i < jsonArray.length(); ++i) {
            String value = jsonArray.getString(i);
            results.add(value);
        }

        return results;
    }

    private void reportError(String message) {
        log.error(message);
        updateLog(message, 3);
        Helper.setFehlerMeldung(message);
        pusher.send("error");
    }

    @Data
    @AllArgsConstructor
    public class ImportSet {
        private String title;
        private String source;
        private String target;
        private boolean person;

        public String toString() {
            return source + " -> " + target;
        }
    }

    @Data
    public class ImportGroupSet {
        private String source;
        private String type;
        private List<ImportSet> elements;

        public ImportGroupSet(String source, String type) {
            this.source = source;
            this.type = type;
            this.elements = new ArrayList<>();
        }

        public void addElement(ImportSet element) {
            elements.add(element);
        }
    }

    @Data
    public class ImportChildDocStruct {
        private String source;
        private String type;
        private List<ImportSet> elements;

        public ImportChildDocStruct(String source, String type) {
            this.source = source;
            this.type = type;
            this.elements = new ArrayList<>();
        }

        public void addElement(ImportSet element) {
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
