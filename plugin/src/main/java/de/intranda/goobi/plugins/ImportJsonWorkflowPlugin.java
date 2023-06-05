package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IPushPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
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
    	
        // read some main configuration
        importFolder = ConfigPlugins.getPluginConfig(title).getString("importFolder");
        workflow = ConfigPlugins.getPluginConfig(title).getString("workflow");
        publicationType = ConfigPlugins.getPluginConfig(title).getString("publicationType");
        jsonFolder = ConfigPlugins.getPluginConfig(title).getString("jsonFolder");
        
        // read list of mapping configuration
        importSets = new ArrayList<>();
        List<HierarchicalConfiguration> mappings = ConfigPlugins.getPluginConfig(title).configurationsAt("importSet");
        for (HierarchicalConfiguration node : mappings) {
            String settitle = node.getString("[@title]", "-");
            String source = node.getString("[@source]", "-");
            String target = node.getString("[@target]", "-");
            boolean person = node.getBoolean("[@person]", false);
            importSets.add(new ImportSet(settitle, source, target, person));
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

                    // create and save the process
                    boolean success = tryCreateAndSaveNewProcess(bhelp, processName, jsonFile);
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
    
    private boolean tryCreateAndSaveNewProcess(BeanHelper bhelp, String processName, Path jsonFile) {
        // get the correct workflow to use
        Process template = ProcessManager.getProcessByExactTitle(workflow);
        // prepare the Fileformat based on the template Process
        Fileformat fileformat = prepareFileformatForNewProcess(template, jsonFile);
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

    private Fileformat prepareFileformatForNewProcess(Process template, Path jsonFile) {
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
            createMetadataFields(prefs, logical, jsonFile);

            return fileformat;

        } catch (PreferencesException | TypeNotAllowedForParentException | MetadataTypeNotAllowedException | IncompletePersonObjectException e) {
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
        File file = new File(importFolder, "file.jpg");
        if (file.canRead()) {
            storageProvider.createDirectories(Paths.get(targetBase));
            storageProvider.copyFile(Paths.get(file.getAbsolutePath()), Paths.get(targetBase, "file.jpg"));
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

    private void createMetadataFields(Prefs prefs, DocStruct ds, Path jsonFile) throws MetadataTypeNotAllowedException {
        for (ImportSet importSet : importSets) {
            // retrieve the value from the configured jsonPath
            String source = importSet.getSource();
            String value = getValueFromSource(source, jsonFile);
            // prepare the MetadataType
            String target = importSet.getTarget();
            MetadataType targetType = prefs.getMetadataTypeByName(target);

            // treat persons different than regular metadata
            if (importSet.isPerson()) {
                updateLog("Add person '" + target + "' with value '" + value + "'");
                Person p = new Person(targetType);
                String firstname = value.substring(0, value.indexOf(" "));
                String lastname = value.substring(value.indexOf(" "));
                p.setFirstname(firstname);
                p.setLastname(lastname);
                ds.addPerson(p);
            } else {
                updateLog("Add metadata '" + target + "' with value '" + value + "'");
                Metadata mdTitle = new Metadata(targetType);
                mdTitle.setValue(source);
                ds.addMetadata(mdTitle);
            }
        }
    }

    private String getValueFromSource(String source, Path jsonFile) {
        log.debug("jsonFile = " + jsonFile);
        // for elements other than arrays, jsonPath should start with $
        if (!source.startsWith("$")) {
            // for those that are not json paths, just return themselves trimmed
            return source.trim();
        }

        // remove the heading $
        String jsonPath = source.substring(1);
        // split jsonPath
        String[] pathArray = jsonPath.split("\\.");
        // iterate over pathArray to get the json object

        return "";
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
    }

    @Data
    @AllArgsConstructor
    public class LogMessage {
        private String message;
        private int level = 0;
    }
}
