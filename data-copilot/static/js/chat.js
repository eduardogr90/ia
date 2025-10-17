/* global INITIAL_CONVERSATIONS */
(() => {
  const conversationList = document.getElementById('conversation-list');
  const messagesContainer = document.getElementById('messages');
  const chatTitle = document.getElementById('chat-title');
  const chatStatus = document.getElementById('chat-status');
  const emptyState = document.getElementById('empty-state');
  const messageForm = document.getElementById('message-form');
  const messageInput = document.getElementById('message-input');
  const submitButton = messageForm.querySelector('button[type="submit"]');
  const newChatButton = document.getElementById('new-chat');
  const THINKING_GIF_URL =
    'https://media.tenor.com/On7kvXhzml4AAAAj/loading-gif.gif';

  let currentConversationId = null;
  let conversations = new Map();

  function showStatus(text, timeout = 2000) {
    chatStatus.textContent = text;
    if (timeout) {
      setTimeout(() => {
        if (chatStatus.textContent === text) {
          chatStatus.textContent = '';
        }
      }, timeout);
    }
  }

  function renderConversationList() {
    conversationList.innerHTML = '';
    const items = Array.from(conversations.values())
      .sort((a, b) => (a.id < b.id ? 1 : -1))
      .map((conversation) => {
        const item = document.createElement('div');
        item.className = 'conversation-item';
        if (conversation.id === currentConversationId) {
          item.classList.add('active');
        }
        const title = document.createElement('span');
        title.textContent = conversation.messages[conversation.messages.length - 1]?.content || conversation.id;
        item.appendChild(title);

        const deleteButton = document.createElement('button');
        deleteButton.type = 'button';
        deleteButton.textContent = '×';
        deleteButton.addEventListener('click', (event) => {
          event.stopPropagation();
          deleteConversation(conversation.id);
        });
        item.appendChild(deleteButton);

        item.addEventListener('click', () => {
          if (conversation.id !== currentConversationId) {
            loadConversation(conversation.id);
          }
        });

        return item;
      });

    if (!items.length) {
      const placeholder = document.createElement('p');
      placeholder.textContent = 'Sin conversaciones';
      placeholder.style.opacity = '0.7';
      placeholder.style.padding = '1rem';
      conversationList.appendChild(placeholder);
    } else {
      items.forEach((item) => conversationList.appendChild(item));
    }
  }

  function renderMessages(conversation) {
    messagesContainer.innerHTML = '';
    if (!conversation || !conversation.messages.length) {
      emptyState.style.display = 'block';
      messagesContainer.appendChild(emptyState);
      return;
    }
    emptyState.style.display = 'none';
    conversation.messages.forEach((message) => {
      const bubble = document.createElement('div');
      bubble.className = `message ${message.role}`;
      if (message.loading) {
        bubble.classList.add('loading');
        const indicator = document.createElement('div');
        indicator.className = 'thinking-indicator';

        const gif = document.createElement('img');
        gif.src = message.gif || THINKING_GIF_URL;
        gif.alt = 'Pensando...';
        gif.className = 'thinking-gif';
        indicator.appendChild(gif);

        const text = document.createElement('span');
        text.textContent = message.content || 'Pensando...';
        indicator.appendChild(text);

        bubble.appendChild(indicator);
      } else {
        bubble.textContent = message.content;
      }
      messagesContainer.appendChild(bubble);
    });
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
  }

  function setActiveConversation(conversation) {
    if (conversation) {
      currentConversationId = conversation.id;
      chatTitle.textContent = `Conversación ${conversation.id}`;
      messageInput.disabled = false;
      submitButton.disabled = false;
      messageInput.focus();
    } else {
      currentConversationId = null;
      chatTitle.textContent = 'Selecciona una conversación';
      messageInput.disabled = true;
      submitButton.disabled = true;
      messageInput.value = '';
    }
    renderConversationList();
    renderMessages(conversation);
  }

  async function newConversation() {
    try {
      const response = await fetch('/new_chat', { method: 'POST' });
      if (!response.ok) {
        throw new Error('Error al crear conversación');
      }
      const conversation = await response.json();
      conversations.set(conversation.id, conversation);
      setActiveConversation(conversation);
      showStatus('Conversación creada');
    } catch (error) {
      console.error(error);
      showStatus('No se pudo crear la conversación', 3000);
    }
  }

  async function loadConversation(convId) {
    try {
      const response = await fetch(`/load_chat/${convId}`);
      if (!response.ok) {
        throw new Error('No se pudo cargar la conversación');
      }
      const conversation = await response.json();
      conversations.set(conversation.id, conversation);
      setActiveConversation(conversation);
    } catch (error) {
      console.error(error);
      showStatus('No se pudo cargar la conversación', 3000);
    }
  }

  async function deleteConversation(convId) {
    if (!confirm('¿Eliminar la conversación de forma permanente?')) {
      return;
    }
    try {
      const response = await fetch(`/delete_chat/${convId}`, { method: 'DELETE' });
      if (!response.ok) {
        throw new Error('No se pudo eliminar la conversación');
      }
      conversations.delete(convId);
      if (currentConversationId === convId) {
        setActiveConversation(null);
      } else {
        renderConversationList();
      }
      showStatus('Conversación eliminada');
    } catch (error) {
      console.error(error);
      showStatus('No se pudo eliminar la conversación', 3000);
    }
  }

  async function sendMessage(message) {
    const conversation = conversations.get(currentConversationId);
    if (!conversation) {
      return;
    }

    const userMessage = { role: 'user', content: message };
    const placeholderMessage = {
      role: 'assistant',
      content: 'Pensando...',
      loading: true,
      gif: THINKING_GIF_URL,
    };

    if (!Array.isArray(conversation.messages)) {
      conversation.messages = [];
    }

    conversation.messages.push(userMessage);
    conversation.messages.push(placeholderMessage);
    renderMessages(conversation);
    renderConversationList();

    try {
      const response = await fetch('/send_message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ conversation_id: currentConversationId, message }),
      });
      if (!response.ok) {
        throw new Error('Error al enviar mensaje');
      }
      const payload = await response.json();
      conversations.set(payload.conversation.id, payload.conversation);
      setActiveConversation(payload.conversation);
    } catch (error) {
      console.error(error);
      showStatus('No se pudo enviar el mensaje', 3000);
      const index = conversation.messages.indexOf(placeholderMessage);
      if (index !== -1) {
        conversation.messages.splice(index, 1);
      }
      const userIndex = conversation.messages.lastIndexOf(userMessage);
      if (userIndex !== -1) {
        conversation.messages.splice(userIndex, 1);
      }
      renderMessages(conversation);
      renderConversationList();
      messageInput.value = message;
      messageInput.focus();
    }
  }

  // Event bindings ----------------------------------------------------
  newChatButton.addEventListener('click', newConversation);

  messageForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const text = messageInput.value.trim();
    if (!text || !currentConversationId) {
      return;
    }
    messageInput.value = '';
    sendMessage(text);
    messageInput.focus();
  });

  messageInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && !event.shiftKey && !submitButton.disabled) {
      event.preventDefault();
      if (typeof messageForm.requestSubmit === 'function') {
        messageForm.requestSubmit();
      } else {
        messageForm.dispatchEvent(new Event('submit', { cancelable: true }));
      }
    }
  });

  // Initial data ------------------------------------------------------
  if (Array.isArray(INITIAL_CONVERSATIONS)) {
    INITIAL_CONVERSATIONS.forEach((conversation) => {
      conversations.set(conversation.id, conversation);
    });
  }
  renderConversationList();
  if (conversations.size) {
    const [firstConversation] = conversations.values();
    setActiveConversation(firstConversation);
  }
})();
