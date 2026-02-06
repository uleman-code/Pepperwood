/**
 * Listens for the uploader becoming "not uploading" and clicks the Done button
 * This is triggered by a Dash callback that sets a data attribute on the uploader
 */
(function initDoneButtonClickListener() {
  const uploader = document.getElementById('select-file');
  if (!uploader) {
    // console.log('[click-done] Uploader not found yet, will retry');
    setTimeout(initDoneButtonClickListener, 500);
    return;
  }

  // console.log('[click-done] Script loaded, watching for upload completion');

  // Use a MutationObserver to watch for when the Done button appears
  const observer = new MutationObserver(function() {
    const doneButton = document.querySelector('.uppy-StatusBar-actionBtn--done');
    
    if (doneButton) {
      // console.log('[click-done] Done button detected, clicking it');
      doneButton.click();
      // console.log('[click-done] Done button clicked');
    }
  });

  // Watch the uploader and its children for changes
  observer.observe(uploader, {
    subtree: true,
    attributes: true,
    attributeFilter: ['class', 'style'],
    childList: true
  });

  // console.log('[click-done] MutationObserver started');
})();
