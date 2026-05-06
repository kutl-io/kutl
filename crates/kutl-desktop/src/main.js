// kutl desktop — setup wizard flow logic.
//
// The wizard has four states: welcome, waiting, folder, done.
// For now (pre-deep-link), the flow skips the "waiting" state
// and goes directly from invite validation to folder picker.

const { invoke } = window.__TAURI__.core;

// ── State management ────────────────────────────────────────────────

/** Space info gathered during the flow. */
const spaceInfo = {
  spaceId: '',
  spaceName: '',
  relayUrl: '',
  folder: '',
  token: null,
  isCreating: false,
};

// ── DOM helpers ─────────────────────────────────────────────────────

/** Show a wizard state and hide all others. */
function showState(stateName) {
  document.querySelectorAll('.wizard-state').forEach((el) => {
    el.classList.toggle('active', el.id === `state-${stateName}`);
  });
}

/** Show an error message within the current state. */
function showError(elementId, message) {
  const el = document.getElementById(elementId);
  if (el) {
    el.textContent = message;
    el.style.display = 'block';
  }
}

/** Clear an error message. */
function clearError(elementId) {
  const el = document.getElementById(elementId);
  if (el) {
    el.textContent = '';
    el.style.display = 'none';
  }
}

/** Enable or disable a button. */
function setButtonLoading(buttonId, loading) {
  const btn = document.getElementById(buttonId);
  if (btn) {
    btn.disabled = loading;
  }
}

// ── Welcome state handlers ──────────────────────────────────────────

/** Handle "Join" button click — validate invite and proceed. */
async function handleJoin() {
  const input = document.getElementById('invite-url');
  const url = input.value.trim();

  if (!url) {
    showError('welcome-error', 'Please paste an invite link.');
    return;
  }

  clearError('welcome-error');
  setButtonLoading('btn-join', true);
  setButtonLoading('btn-create', true);

  try {
    const info = await invoke('validate_invite', { url });
    spaceInfo.spaceId = info.space_id;
    spaceInfo.spaceName = info.space_name;
    spaceInfo.relayUrl = info.relay_url;

    // Set default folder to ~/kutl/<space_name>.
    const home = await invoke('get_home_dir');
    spaceInfo.folder = `${home}/kutl/${info.space_name}`;

    showFolderPicker();
  } catch (err) {
    showError('welcome-error', String(err));
  } finally {
    setButtonLoading('btn-join', false);
    setButtonLoading('btn-create', false);
  }
}

/** Handle "Create a new space" button click. */
async function handleCreateSpace() {
  clearError('welcome-error');

  // Switch to a simple inline prompt for the space name.
  const name = prompt('Space name (lowercase, hyphens ok):');
  if (!name || !name.trim()) return;

  const trimmed = name.trim().toLowerCase();

  // Prompt for relay URL (default to localhost for dev).
  const relay = prompt('Relay URL:', 'http://localhost:9100');
  if (!relay || !relay.trim()) return;

  spaceInfo.spaceName = trimmed;
  spaceInfo.relayUrl = relay.trim();

  // Set default folder.
  try {
    const home = await invoke('get_home_dir');
    spaceInfo.folder = `${home}/kutl/${trimmed}`;
  } catch (_e) {
    spaceInfo.folder = `/tmp/kutl/${trimmed}`;
  }

  // Show folder picker — the actual space registration happens at "Start syncing".
  spaceInfo.isCreating = true;
  showFolderPicker();
}

// ── Folder picker state ─────────────────────────────────────────────

/** Populate and show the folder picker state. */
function showFolderPicker() {
  document.getElementById('folder-space-name').textContent = spaceInfo.spaceName;
  document.getElementById('folder-relay-host').textContent = extractHost(spaceInfo.relayUrl);
  document.getElementById('folder-path-display').textContent = spaceInfo.folder;

  showState('folder');
}

/** Handle "Browse..." button — open native folder picker. */
async function handleBrowse() {
  try {
    const path = await invoke('pick_folder');
    if (path) {
      spaceInfo.folder = path;
      document.getElementById('folder-path-display').textContent = path;
    }
  } catch (err) {
    showError('folder-error', `Failed to open folder picker: ${err}`);
  }
}

/** Handle "Start syncing" button — complete the join/create flow. */
async function handleStartSyncing() {
  clearError('folder-error');
  setButtonLoading('btn-start-syncing', true);

  try {
    let result;
    if (spaceInfo.isCreating) {
      result = await invoke('create_space', {
        name: spaceInfo.spaceName,
        relayUrl: spaceInfo.relayUrl,
        folder: spaceInfo.folder,
      });
    } else {
      result = await invoke('complete_join', {
        spaceId: spaceInfo.spaceId,
        spaceName: spaceInfo.spaceName,
        relayUrl: spaceInfo.relayUrl,
        token: spaceInfo.token,
        folder: spaceInfo.folder,
      });
    }

    // Populate done state.
    spaceInfo.spaceId = result.space_id;
    spaceInfo.spaceName = result.space_name;
    spaceInfo.folder = result.folder;

    document.getElementById('done-space-name').textContent = result.space_name;
    document.getElementById('done-folder').textContent = result.folder;

    showState('done');
  } catch (err) {
    showError('folder-error', String(err));
  } finally {
    setButtonLoading('btn-start-syncing', false);
  }
}

// ── Done state handlers ─────────────────────────────────────────────

/** Open the synced folder in the system file manager. */
async function handleOpenFolder() {
  try {
    await invoke('open_folder', { path: spaceInfo.folder });
  } catch (_err) {
    // Best-effort — ignore if it fails.
  }
}

/** Handle "Done" / close wizard. */
async function handleClose() {
  // Close the current webview window via the Tauri API if available,
  // otherwise just hide the wizard UI.
  try {
    const { getCurrentWindow } = window.__TAURI__.window;
    if (getCurrentWindow) {
      await getCurrentWindow().close();
    }
  } catch (_e) {
    // If the window API isn't available, just reset to welcome state.
    showState('welcome');
  }
}

// ── Utilities ───────────────────────────────────────────────────────

/** Extract the host portion from a URL for display. */
function extractHost(url) {
  try {
    const parsed = new URL(url);
    return parsed.host;
  } catch (_e) {
    return url;
  }
}

// ── Deep link handler ────────────────────────────────────────────────

/**
 * Handle a validated `kutl://join` deep link emitted by the Rust backend.
 *
 * Populates spaceInfo from the event payload and advances the wizard to the
 * folder picker, regardless of which state the wizard is currently in.
 */
async function handleDeepLinkJoin(payload) {
  spaceInfo.spaceId = payload.spaceId ?? '';
  spaceInfo.spaceName = payload.spaceName ?? '';
  spaceInfo.relayUrl = payload.relay ?? '';
  spaceInfo.token = payload.token ?? null;
  spaceInfo.isCreating = false;

  // Set a sensible default folder path.
  try {
    const home = await invoke('get_home_dir');
    spaceInfo.folder = `${home}/kutl/${spaceInfo.spaceName}`;
  } catch (_e) {
    spaceInfo.folder = `/tmp/kutl/${spaceInfo.spaceName}`;
  }

  showFolderPicker();
}

// ── Initialize ──────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  // Welcome state handlers.
  document.getElementById('btn-join').addEventListener('click', handleJoin);
  document.getElementById('btn-create').addEventListener('click', handleCreateSpace);

  // Allow Enter key in the invite URL field.
  document.getElementById('invite-url').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') handleJoin();
  });

  // Folder state handlers.
  document.getElementById('btn-browse').addEventListener('click', handleBrowse);
  document.getElementById('btn-start-syncing').addEventListener('click', handleStartSyncing);
  document.getElementById('btn-back-to-welcome').addEventListener('click', () => {
    spaceInfo.isCreating = false;
    showState('welcome');
  });

  // Done state handlers.
  document.getElementById('btn-open-folder').addEventListener('click', handleOpenFolder);
  document.getElementById('btn-close').addEventListener('click', handleClose);

  // Listen for validated deep-link-join events emitted by the Rust backend.
  // The backend validates relay scheme and space_name before emitting, so we
  // trust the payload here and go straight to the folder picker.
  const { listen } = window.__TAURI__.event;
  listen('deep-link-join', (event) => {
    handleDeepLinkJoin(event.payload);
  });

  // Show initial state.
  showState('welcome');
});
