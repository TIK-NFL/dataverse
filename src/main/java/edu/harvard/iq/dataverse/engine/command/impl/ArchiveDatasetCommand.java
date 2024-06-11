package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetLock;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.RequiredPermissions;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.exception.IllegalCommandException;
import edu.harvard.iq.dataverse.workflow.Workflow;
import edu.harvard.iq.dataverse.workflow.WorkflowContext.TriggerType;
import java.util.Optional;
import java.util.logging.Logger;
import static java.util.stream.Collectors.joining;
import static edu.harvard.iq.dataverse.engine.command.impl.ArchiveDatasetResult.Status;
import static edu.harvard.iq.dataverse.dataset.DatasetUtil.validateDatasetMetadataExternally;
import edu.harvard.iq.dataverse.util.StringUtil;


/**
 * Kick-off a dataset publication process. The process may complete immediately, 
 * but may also result in a workflow being started and pending on some external 
 * response. Either way, the process will be completed by an instance of 
 * {@link FinalizeDatasetPublicationCommand}.
 * 
 * @see FinalizeDatasetPublicationCommand
 * 
 * @author skraffmiller
 * @author michbarsinai
 * @author Florian Fritze <florian.fritze@ub.uni-stuttgart.de>
 */
@RequiredPermissions(Permission.PublishDataset)
public class ArchiveDatasetCommand extends AbstractPublishDatasetCommand<ArchiveDatasetResult> {
    private static final Logger logger = Logger.getLogger(ArchiveDatasetCommand.class.getName());
    DataverseRequest request;
    
    final boolean datasetExternallyReleased = false;

    public ArchiveDatasetCommand(Dataset datasetIn, DataverseRequest aRequest) {
        super(datasetIn, aRequest);
        this.request = aRequest;
    }
    
    /** 
     * The dataset was already released by an external system, and now Dataverse
     * is just internally marking this release version as released. This is happening
     * in scenarios like import or migration.
     */
    
    @Override
    public ArchiveDatasetResult execute(CommandContext ctxt) throws CommandException {
        
        verifyCommandArguments(ctxt);
        
        // Invariant 1: If we're here, publishing the dataset makes sense, from a "business logic" point of view.
        // Invariant 2: The latest version of the dataset is the one being published, EVEN IF IT IS NOT DRAFT.
        //              When importing a released dataset, the latest version is marked as RELEASED.

        Dataset theDataset = getDataset();
        
        validateOrDie(theDataset.getLatestVersion(), false);

        //ToDo - any reason to set the version in publish versus finalize? Failure in a prepub workflow or finalize will leave draft versions with an assigned version number as is.
        //Changing the dataset in this transaction also potentially makes a race condition with a prepub workflow, possibly resulting in an OptimisticLockException there.
        
        // Set the version numbers:

        if (theDataset.getPublicationDate() == null) {
            // First Release
            theDataset.getLatestVersion().setVersionNumber(Long.valueOf(1)); // minor release is blocked by #verifyCommandArguments
            theDataset.getLatestVersion().setMinorVersionNumber(Long.valueOf(0));
            
        } else {
            // major, non-first release
            theDataset.getLatestVersion().setVersionNumber(Long.valueOf(theDataset.getVersionNumber() + 1));
            theDataset.getLatestVersion().setMinorVersionNumber(Long.valueOf(0));
        }
        
        // Perform any optional validation steps, if defined:
        if (ctxt.systemConfig().isExternalDatasetValidationEnabled()) {
            // For admins, an override of the external validation step may be enabled: 
            if (!(getUser().isSuperuser() && ctxt.systemConfig().isExternalValidationAdminOverrideEnabled())) {
                String executable = ctxt.systemConfig().getDatasetValidationExecutable();
                boolean result = validateDatasetMetadataExternally(theDataset, executable, getRequest());
            
                if (!result) {
                    String rejectionMessage = ctxt.systemConfig().getDatasetValidationFailureMsg();
                    throw new IllegalCommandException(rejectionMessage, this);
                }
            } 
        }
        
        //ToDo - should this be in onSuccess()? May relate to todo above 
        Optional<Workflow> archPubWf = ctxt.workflows().getDefaultWorkflow(TriggerType.ArchiveDataset);
        if ( archPubWf.isPresent() ) {
            // We start a workflow
            theDataset = ctxt.em().merge(theDataset);
            ctxt.em().flush();
            ctxt.workflows().start(archPubWf.get(), buildContext(theDataset, TriggerType.ArchiveDataset, datasetExternallyReleased), true);
            return new ArchiveDatasetResult(theDataset, Status.Workflow);
            
        } else {
            // We will skip trying to register the global identifiers for datafiles 
            // if "dependent" file-level identifiers are requested, AND the naming 
            // protocol of the dataset global id is different from the 
            // one currently configured for the Dataverse. This is to specifically 
            // address the issue with the datasets with handle ids registered, 
            // that are currently configured to use DOI.
            // If we are registering file-level identifiers, and there are more 
            // than the configured limit number of files, then call Finalize 
            // asychronously (default is 10)
            // ...
            // Additionaly in 4.9.3 we have added a system variable to disable 
            // registering file PIDs on the installation level.

            boolean validatePhysicalFiles = ctxt.systemConfig().isDatafileValidationOnPublishEnabled();

            // As of v5.0, publishing a dataset is always done asynchronously, 
            // with the dataset locked for the duration of the operation. 
            String info = "Archiving the dataset; ";
            info += validatePhysicalFiles ? "Validating Datafiles Asynchronously" : "";

            AuthenticatedUser user = request.getAuthenticatedUser();
            /*
                 * datasetExternallyReleased is only true in the case of the
                 * Dataverses.importDataset() and importDatasetDDI() methods. In that case, we
                 * are still in the transaction that creates theDataset, so
                 * A) Trying to create a DatasetLock referncing that dataset in a new 
                 * transaction (as ctxt.datasets().addDatasetLock() does) will fail since the 
                 * dataset doesn't yet exist, and 
                 * B) a lock isn't needed because no one can be trying to edit it yet (as it
                 * doesn't exist).
                 * Thus, we can/need to skip creating the lock. Since the calls to removeLocks
                 * in FinalizeDatasetPublicationCommand search for and remove existing locks, if
                 * one doesn't exist, the removal is a no-op in this case.
             */
            if (!datasetExternallyReleased) {
                DatasetLock lock = new DatasetLock(DatasetLock.Reason.finalizePublication, user);
                lock.setDataset(theDataset);
                lock.setInfo(info);
                ctxt.datasets().addDatasetLock(theDataset, lock);
            }
            theDataset = ctxt.em().merge(theDataset);
            // The call to FinalizePublicationCommand has been moved to the new @onSuccess()
            // method:
            //ctxt.datasets().callFinalizePublishCommandAsynchronously(theDataset.getId(), ctxt, request, datasetExternallyReleased);
            return new ArchiveDatasetResult(theDataset, Status.Inprogress);
        }
    }
    
    /**
     * See that publishing the dataset in the requested manner makes sense, at
     * the given state of the dataset.
     * 
     * @throws IllegalCommandException if the publication request is invalid.
     */
    private void verifyCommandArguments(CommandContext ctxt) throws IllegalCommandException {
        if (!getDataset().getOwner().isReleased()) {
            throw new IllegalCommandException("This dataset may not be published because its host dataverse (" + getDataset().getOwner().getAlias() + ") has not been published.", this);
        }
        
        if ( ! getUser().isAuthenticated() ) {
            throw new IllegalCommandException("Only authenticated users can release a Dataset. Please authenticate and try again.", this);
        }
        
        if (getDataset().getLatestVersion().getTermsOfUseAndAccess() == null
                || (getDataset().getLatestVersion().getTermsOfUseAndAccess().getLicense() == null 
                && StringUtil.isEmpty(getDataset().getLatestVersion().getTermsOfUseAndAccess().getTermsOfUse()))) {
            throw new IllegalCommandException("Dataset must have a valid license or Custom Terms Of Use configured before it can be published.", this);
        }
        
        if ( (getDataset().isLockedFor(DatasetLock.Reason.Workflow)&&!ctxt.permissions().isMatchingWorkflowLock(getDataset(),request.getUser().getIdentifier(),request.getWFInvocationId())) 
                || getDataset().isLockedFor(DatasetLock.Reason.Ingest) 
                || getDataset().isLockedFor(DatasetLock.Reason.finalizePublication)
                || getDataset().isLockedFor(DatasetLock.Reason.EditInProgress)) {
            throw new IllegalCommandException("This dataset is locked. Reason: " 
                    + getDataset().getLocks().stream().map(l -> l.getReason().name()).collect( joining(",") )
                    + ". Please try publishing later.", this);
        }
        
        if ( getDataset().isLockedFor(DatasetLock.Reason.FileValidationFailed)) {
            throw new IllegalCommandException("This dataset cannot be archived because some files have been found missing or corrupted. " 
                    + ". Please contact support to address this.", this);
        }
        
        if ( datasetExternallyReleased ) {
            if ( ! getDataset().getLatestVersion().isReleased() ) {
                throw new IllegalCommandException("Latest version of dataset " + getDataset().getIdentifier() + " is not marked as released.", this);
            }
                
        } else {
            if (getDataset().getLatestVersion().isReleased()) {
                throw new IllegalCommandException("Latest version of dataset " + getDataset().getIdentifier() + " is already released. Only draft versions can be released.", this);
            }

        }
    }
    
    
    @Override
    public boolean onSuccess(CommandContext ctxt, Object r) {
        Dataset dataset = null;
        try{
            dataset = (Dataset) r;
        } catch (ClassCastException e){
            dataset  = ((ArchiveDatasetResult) r).getDataset();
        }

        if (dataset != null) {
            Optional<Workflow> archPubWf = ctxt.workflows().getDefaultWorkflow(TriggerType.ArchiveDataset);
            //A pre-publication workflow will call FinalizeDatasetPublicationCommand itself when it completes
            if (! archPubWf.isPresent() ) {
                logger.fine("From onSuccess, calling FinalizeArchiveCommand for dataset " + dataset.getGlobalId().asString());
                ctxt.datasets().callFinalizeArchiveCommandAsynchronously(dataset.getId(), ctxt, request);
                return true;
            }
        }
        
        return false;
    }
    
}
