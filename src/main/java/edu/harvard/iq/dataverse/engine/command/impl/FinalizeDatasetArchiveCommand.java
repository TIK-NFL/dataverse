package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.ControlledVocabularyValue;
import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetField;
import edu.harvard.iq.dataverse.DatasetFieldConstant;
import edu.harvard.iq.dataverse.DatasetLock;
import static edu.harvard.iq.dataverse.DatasetVersion.VersionState.*;
import edu.harvard.iq.dataverse.DatasetVersionUser;
import edu.harvard.iq.dataverse.Dataverse;
import edu.harvard.iq.dataverse.DvObject;
import edu.harvard.iq.dataverse.Embargo;
import edu.harvard.iq.dataverse.UserNotification;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.dataset.DatasetUtil;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.RequiredPermissions;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
//import edu.harvard.iq.dataverse.export.ExportService;
import edu.harvard.iq.dataverse.privateurl.PrivateUrl;
import edu.harvard.iq.dataverse.util.BundleUtil;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
//import java.util.logging.Level;
import java.util.logging.Logger;

import edu.harvard.iq.dataverse.batch.util.LoggingUtil;
import edu.harvard.iq.dataverse.dataaccess.StorageIO;
import edu.harvard.iq.dataverse.engine.command.Command;
import edu.harvard.iq.dataverse.util.FileUtil;
import java.util.ArrayList;
import java.util.concurrent.Future;
import org.apache.solr.client.solrj.SolrServerException;


/**
 *
 * Takes the last internal steps in publishing a dataset.
 *
 * @author michael
 */
@RequiredPermissions(Permission.PublishDataset)
public class FinalizeDatasetArchiveCommand extends AbstractPublishDatasetCommand<Dataset> {

    private static final Logger logger = Logger.getLogger(FinalizeDatasetArchiveCommand.class.getName());



    List<Dataverse> dataversesToIndex = new ArrayList<>();
    
    public static final String FILE_VALIDATION_ERROR = "FILE VALIDATION ERROR";
    
    public FinalizeDatasetArchiveCommand(Dataset aDataset, DataverseRequest aRequest) {
        super(aDataset, aRequest);
    }

    @Override
    public Dataset execute(CommandContext ctxt) throws CommandException {
        Dataset theDataset = getDataset();
        
        logger.info("Finalizing archive of the dataset "+theDataset.getGlobalId().asString());
        
        // validate the physical files before we do anything else: 
        // (unless specifically disabled; or a minor version)
        if (theDataset.getLatestVersion().getVersionState() != LONGTERM_ARCHIVED
                && theDataset.getLatestVersion().getMinorVersionNumber() != null
                && theDataset.getLatestVersion().getMinorVersionNumber().equals((long) 0)
                && ctxt.systemConfig().isDatafileValidationOnPublishEnabled()) {
            // some imported datasets may already be released.

            // validate the physical files (verify checksums):
            validateDataFiles(theDataset, ctxt);
            // (this will throw a CommandException if it fails)
        }
        
        validateOrDie(theDataset.getLatestVersion(), false);
        
                
        // is this the first publication of the dataset?
        if (theDataset.getPublicationDate() == null) {
            theDataset.setReleaseUser((AuthenticatedUser) getUser());
        
            theDataset.setPublicationDate(new Timestamp(new Date().getTime()));
            
            // if there are any embargoed files in this version, we will save 
            // the latest availability date as the "embargoCitationDate" for future 
            // reference (if the files are not available yet, as of publishing of 
            // the dataset, this date will be used as the "ciatation date" of the dataset, 
            // instead of the publicatonDate, in compliance with the DataCite 
            // best practices). 
            // the code below replicates the logic that used to be in the method 
            // Dataset.getCitationDate() that calculated this adjusted date in real time.
            
//            Timestamp latestEmbargoDate = null; 
//            for (DataFile dataFile : theDataset.getFiles()) {
//                // this is the first version of the dataset that is being published. 
//                // therefore we can iterate through .getFiles() instead of obtaining
//                // the DataFiles by going through the FileMetadatas in the current version.
//                Embargo embargo = dataFile.getEmbargo();
//                if (embargo != null) {
//                    // "dataAvailable" is not nullable in the Embargo class, no need for a null check
//                    Timestamp embargoDate = Timestamp.valueOf(embargo.getDateAvailable().atStartOfDay());
//                    if (latestEmbargoDate == null || latestEmbargoDate.compareTo(embargoDate) < 0) {
//                        latestEmbargoDate = embargoDate;
//                    }
//                }
//            }
//            // the above loop could be easily replaced with a database query; 
//            // but we iterate through .getFiles() elsewhere in the command, when 
//            // updating and/or registering the files, so it should not result in 
//            // an extra performance hit. 
//            theDataset.setEmbargoCitationDate(latestEmbargoDate);
        } 

        //Clear any external status
        theDataset.getLatestVersion().setExternalStatusLabel(null);
        
        // update metadata
        if (theDataset.getLatestVersion().getReleaseTime() == null) {
            // Allow migrated versions to keep original release dates
            theDataset.getLatestVersion().setReleaseTime(getTimestamp());
        }
        theDataset.getLatestVersion().setLastUpdateTime(getTimestamp());
        theDataset.setModificationTime(getTimestamp());
        theDataset.setFileAccessRequest(theDataset.getLatestVersion().getTermsOfUseAndAccess().isFileAccessRequest());
        
        //Use dataset pub date (which may not be the current date for migrated datasets)
        updateFiles(new Timestamp(theDataset.getLatestVersion().getReleaseTime().getTime()), ctxt);
        
        // 
        // TODO: Not sure if this .merge() is necessary here - ? 
        // I'm moving a bunch of code from PublishDatasetCommand here; and this .merge()
        // comes from there. There's a chance that the final merge, at the end of this
        // command, would be sufficient. -- L.A. Sep. 6 2017
        theDataset = ctxt.em().merge(theDataset);
        setDataset(theDataset);
        updateDatasetUser(ctxt);
        
        //if the publisher hasn't contributed to this version
        DatasetVersionUser ddu = ctxt.datasets().getDatasetVersionUser(theDataset.getLatestVersion(), getUser());
        
        if (ddu == null) {
            ddu = new DatasetVersionUser();
            ddu.setDatasetVersion(theDataset.getLatestVersion());
            String id = getUser().getIdentifier();
            id = id.startsWith("@") ? id.substring(1) : id;
            AuthenticatedUser au = ctxt.authentication().getAuthenticatedUser(id);
            ddu.setAuthenticatedUser(au);
        }
        ddu.setLastUpdateDate(getTimestamp());
        ctxt.em().merge(ddu);
        
        try {
            updateParentDataversesSubjectsField(theDataset, ctxt);
        } catch (IOException | SolrServerException e) {
            String failureLogText = "Post-publication indexing failed for Dataverse subject update. ";
            failureLogText += "\r\n" + e.getLocalizedMessage();
            LoggingUtil.writeOnSuccessFailureLog(this, failureLogText, theDataset);

        }

        List<Command> previouslyCalled = ctxt.getCommandsCalled();
        
        PrivateUrl privateUrl = ctxt.engine().submit(new GetPrivateUrlCommand(getRequest(), theDataset));
        List<Command> afterSub = ctxt.getCommandsCalled();
        previouslyCalled.forEach((c) -> {
            ctxt.getCommandsCalled().add(c);
        });
        if (privateUrl != null) {
            ctxt.engine().submit(new DeletePrivateUrlCommand(getRequest(), theDataset));
        }
        
	if (theDataset.getLatestVersion().getVersionState() != LONGTERM_ARCHIVED) {
            // some imported datasets may already be released.

                // Will throw a CommandException, unless successful.
                // This will end the execution of the command, but the method 
                // above takes proper care to "clean up after itself" in case of
                // a failure - it will remove any locks, and it will send a
                // proper notification to the user(s). 
            theDataset.getLatestVersion().setVersionState(LONGTERM_ARCHIVED);
        }
        
        final Dataset ds = ctxt.em().merge(theDataset);
        //Remove any pre-pub workflow lock (not needed as WorkflowServiceBean.workflowComplete() should already have removed it after setting the finalizePublication lock?)
        ctxt.datasets().removeDatasetLocks(ds, DatasetLock.Reason.Workflow);
        
        Dataset readyDataset = ctxt.em().merge(ds);
        
        // Finally, unlock the dataset (leaving any post-publish workflow lock in place)
        ctxt.datasets().removeDatasetLocks(readyDataset, DatasetLock.Reason.finalizePublication);
        if (readyDataset.isLockedFor(DatasetLock.Reason.InReview) ) {
            ctxt.datasets().removeDatasetLocks(readyDataset, DatasetLock.Reason.InReview);
        }
        
        logger.info("Successfully archived the dataset "+readyDataset.getGlobalId().asString());
        readyDataset = ctxt.em().merge(readyDataset);
        
        return readyDataset;
    }
    
    @Override
    public boolean onSuccess(CommandContext ctxt, Object r) {
        boolean retVal = true;
        Dataset dataset = null;
        try{
            dataset = (Dataset) r;
        } catch (ClassCastException e){
            dataset  = ((ArchiveDatasetResult) r).getDataset();
        }
        
        try {
            // Success! - send notification:
            notifyUsersDatasetPublishStatus(ctxt, dataset, UserNotification.Type.PUBLISHEDDS);
        } catch (Exception e) {
            logger.warning("Failure to send dataset published messages for : " + dataset.getId() + " : " + e.getMessage());
        }
        ctxt.index().asyncIndexDataset(dataset, true);                   
        
        //re-indexing dataverses that have additional subjects
        if (!dataversesToIndex.isEmpty()){
            for (Dataverse dv : dataversesToIndex) {
                try {
                    Future<String> indexString = ctxt.index().indexDataverse(dv);
                } catch (IOException | SolrServerException e) {
                    String failureLogText = "Post-publication indexing failed. You can kick off a re-index of this dataverse with: \r\n curl http://localhost:8080/api/admin/index/dataverses/" + dv.getId().toString();
                    failureLogText += "\r\n" + e.getLocalizedMessage();
                    LoggingUtil.writeOnSuccessFailureLog(this, failureLogText, dataset);
                    retVal = false;
                } 
            }
        }

        // Metadata export:
        
//        try {
//            ExportService instance = ExportService.getInstance();
//            instance.exportAllFormats(dataset);
//            dataset = ctxt.datasets().merge(dataset); 
//        } catch (Exception ex) {
//            // Something went wrong!
//            // Just like with indexing, a failure to export is not a fatal
//            // condition. We'll just log the error as a warning and keep
//            // going:
//            logger.log(Level.WARNING, "Finalization: exception caught while exporting: "+ex.getMessage(), ex);
//            // ... but it is important to only update the export time stamp if the 
//            // export was indeed successful.
//        }        
        
        return retVal;
    }

    /**
     * add the dataset subjects to all parent dataverses.
     */
    private void updateParentDataversesSubjectsField(Dataset savedDataset, CommandContext ctxt) throws  SolrServerException, IOException {
        
        for (DatasetField dsf : savedDataset.getLatestVersion().getDatasetFields()) {
            if (dsf.getDatasetFieldType().getName().equals(DatasetFieldConstant.subject)) {
                Dataverse dv = savedDataset.getOwner();
                while (dv != null) {
                    boolean newSubjectsAdded = false;
                    for (ControlledVocabularyValue cvv : dsf.getControlledVocabularyValues()) {                   
                        if (!dv.getDataverseSubjects().contains(cvv)) {
                            logger.fine("dv "+dv.getAlias()+" does not have subject "+cvv.getStrValue());
                            newSubjectsAdded = true;
                            dv.getDataverseSubjects().add(cvv);
                        } else {
                            logger.fine("dv "+dv.getAlias()+" already has subject "+cvv.getStrValue());
                        }
                    }
                    if (newSubjectsAdded) {
                        logger.fine("new dataverse subjects added - saving and reindexing in OnSuccess");
                        Dataverse dvWithSubjectJustAdded = ctxt.em().merge(dv);
                        ctxt.em().flush();
                        //adding dv to list of those we need to re-index for new subjects
                        dataversesToIndex.add(dvWithSubjectJustAdded);                       
                    } else {
                        logger.fine("no new subjects added to the dataverse; skipping reindexing");
                    }
                    dv = dv.getOwner();
                }
                break; // we just update the field whose name is DatasetFieldConstant.subject
            }
        }
    }

    private void validateDataFiles(Dataset dataset, CommandContext ctxt) throws CommandException {
        try {
            long maxDatasetSize = ctxt.systemConfig().getDatasetValidationSizeLimit();
            long maxFileSize = ctxt.systemConfig().getFileValidationSizeLimit();

            long datasetSize = DatasetUtil.getDownloadSizeNumeric(dataset.getLatestVersion(), false);
            if (maxDatasetSize == -1 || datasetSize < maxDatasetSize) {
                for (DataFile dataFile : dataset.getFiles()) {
                    // TODO: Should we validate all the files in the dataset, or only
                    // the files that haven't been published previously?
                    // (the decision was made to validate all the files on every
                    // major release; we can revisit the decision if there's any
                    // indication that this makes publishing take significantly longer.
                    String driverId = FileUtil.getStorageDriver(dataFile);
                    if(StorageIO.isDataverseAccessible(driverId) && (maxFileSize == -1 || dataFile.getFilesize() < maxFileSize)) {
                        FileUtil.validateDataFileChecksum(dataFile);
                    }
                    else {
                        String message = "Checksum Validation skipped for this datafile: " + dataFile.getId() + ", because of the size of the datafile limit (set to " + maxFileSize + " ); ";
                        logger.info(message);
                    }
                }
            }
            else {
                String message = "Checksum Validation skipped for this dataset: " + dataset.getId() + ", because of the size of the dataset limit (set to " + maxDatasetSize + " ); ";
                logger.info(message);
            }
        } catch (Throwable e) {
            if (dataset.isLockedFor(DatasetLock.Reason.finalizePublication)) {
                DatasetLock lock = dataset.getLockFor(DatasetLock.Reason.finalizePublication);
                lock.setReason(DatasetLock.Reason.FileValidationFailed);
                lock.setInfo(FILE_VALIDATION_ERROR);
                ctxt.datasets().updateDatasetLock(lock);
            } else {            
                // Lock the dataset with a new FileValidationFailed lock: 
                DatasetLock lock = new DatasetLock(DatasetLock.Reason.FileValidationFailed, getRequest().getAuthenticatedUser()); //(AuthenticatedUser)getUser());
                lock.setDataset(dataset);
                lock.setInfo(FILE_VALIDATION_ERROR);
                ctxt.datasets().addDatasetLock(dataset, lock);
            }
            
            // Throw a new CommandException; if the command is being called 
            // synchronously, it will be intercepted and the page will display 
            // the error message for the user.
            throw new CommandException(BundleUtil.getStringFromBundle("dataset.publish.file.validation.error.details"), this);
        }
    }
    
    private void updateFiles(Timestamp updateTime, CommandContext ctxt) throws CommandException {
        for (DataFile dataFile : getDataset().getFiles()) {
            if (dataFile.getPublicationDate() == null) {
                // this is a new, previously unpublished file, so publish by setting date
                dataFile.setPublicationDate(updateTime);
                
                // check if any prexisting roleassignments have file download and send notifications
                notifyUsersFileDownload(ctxt, dataFile);
            }
            
            // set the files restriction flag to the same as the latest version's
            if (dataFile.getFileMetadata() != null && dataFile.getFileMetadata().getDatasetVersion().equals(getDataset().getLatestVersion())) {
                dataFile.setRestricted(dataFile.getFileMetadata().isRestricted());
            }
            
            
            if (dataFile.isRestricted()) {
                // If the file has been restricted: 
                //    If this (image) file has been assigned as the dedicated 
                //    thumbnail for the dataset, we need to remove that assignment, 
                //    now that the file is restricted. 
               
                // Dataset thumbnail assignment: 
                
                if (dataFile.equals(getDataset().getThumbnailFile())) {
                    getDataset().setThumbnailFile(null);
                }
            }
        }
    }
    
   
    //These notification methods are fairly similar, but it was cleaner to create a few copies.
    //If more notifications are needed in this command, they should probably be collapsed.
    private void notifyUsersFileDownload(CommandContext ctxt, DvObject subject) {
        ctxt.roles().directRoleAssignments(subject).stream()
            .filter(  ra -> ra.getRole().permissions().contains(Permission.DownloadFile) )
            .flatMap( ra -> ctxt.roleAssignees().getExplicitUsers(ctxt.roleAssignees().getRoleAssignee(ra.getAssigneeIdentifier())).stream() )
            .distinct() // prevent double-send
            .forEach( au -> ctxt.notifications().sendNotification(au, getTimestamp(), UserNotification.Type.GRANTFILEACCESS, getDataset().getId()) );
    }
    
    private void notifyUsersDatasetPublishStatus(CommandContext ctxt, DvObject subject, UserNotification.Type type) {
        
        ctxt.roles().rolesAssignments(subject).stream()
            .filter(  ra -> ra.getRole().permissions().contains(Permission.ViewUnpublishedDataset) || ra.getRole().permissions().contains(Permission.DownloadFile))
            .flatMap( ra -> ctxt.roleAssignees().getExplicitUsers(ctxt.roleAssignees().getRoleAssignee(ra.getAssigneeIdentifier())).stream() )
            .distinct() // prevent double-send
            //.forEach( au -> ctxt.notifications().sendNotification(au, timestamp, messageType, theDataset.getId()) ); //not sure why this line doesn't work instead
            .forEach( au -> ctxt.notifications().sendNotificationInNewTransaction(au, getTimestamp(), type, getDataset().getLatestVersion().getId()) ); 
    }

}
