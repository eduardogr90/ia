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

  const chartInstances = {};

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

  function destroyChart(chartId) {
    if (chartInstances[chartId]) {
      chartInstances[chartId].destroy();
      delete chartInstances[chartId];
    }
  }

  function escapeHTML(text) {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function renderMarkdown(content) {
    if (!content) {
      return '';
    }

    const text = String(content);

    if (typeof marked !== 'undefined') {
      const html = typeof marked.parse === 'function' ? marked.parse(text) : marked(text);
      if (typeof DOMPurify !== 'undefined') {
        return DOMPurify.sanitize(html, { USE_PROFILES: { html: true } });
      }
      return html;
    }

    // Fallback: escape HTML and convert new lines to <br>
    return escapeHTML(text).replace(/\n/g, '<br>');
  }

  function createChartTable(chart) {
    if (
      !chart ||
      !Array.isArray(chart.labels) ||
      !Array.isArray(chart.values) ||
      chart.labels.length !== chart.values.length
    ) {
      return null;
    }

    const table = document.createElement('table');
    table.className = 'chart-data-table';

    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');

    const labelHeader = document.createElement('th');
    labelHeader.textContent = chart.labelHeader || 'Categoría';
    headRow.appendChild(labelHeader);

    const valueHeader = document.createElement('th');
    valueHeader.textContent = chart.valueHeader || 'Valor';
    headRow.appendChild(valueHeader);

    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    chart.labels.forEach((label, index) => {
      const row = document.createElement('tr');
      const labelCell = document.createElement('td');
      labelCell.textContent = label;
      row.appendChild(labelCell);

      const valueCell = document.createElement('td');
      valueCell.textContent = chart.values[index];
      row.appendChild(valueCell);

      tbody.appendChild(row);
    });

    table.appendChild(tbody);

    return table;
  }

  function renderMessages(conversation) {
    Object.keys(chartInstances).forEach((chartId) => destroyChart(chartId));
    messagesContainer.innerHTML = '';
    if (!conversation || !conversation.messages.length) {
      emptyState.style.display = 'block';
      messagesContainer.appendChild(emptyState);
      return;
    }
    emptyState.style.display = 'none';
    conversation.messages.forEach((message, index) => {
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
        const contentWrapper = document.createElement('div');
        contentWrapper.className = 'message-content';

        if (message.role === 'assistant') {
          contentWrapper.innerHTML = renderMarkdown(message.content);
        } else {
          contentWrapper.classList.add('plain-text');
          contentWrapper.textContent = message.content || '';
        }

        bubble.appendChild(contentWrapper);
      }

      if (
        message.chart &&
        typeof window.Chart !== 'undefined' &&
        Array.isArray(message.chart.labels) &&
        Array.isArray(message.chart.values)
      ) {
        const chartContainer = document.createElement('div');
        chartContainer.className = 'chart-container';
        const canvas = document.createElement('canvas');
        const chartId = `chart-${conversation.id}-${index}`;
        canvas.id = chartId;
        chartContainer.appendChild(canvas);
        bubble.appendChild(chartContainer);

        requestAnimationFrame(() => {
          const context = canvas.getContext('2d');
          if (!context) {
            return;
          }
          destroyChart(chartId);
          chartInstances[chartId] = new Chart(context, {
            type: 'bar',
            data: {
              labels: message.chart.labels,
              datasets: [
                {
                  label: message.chart.label || 'Resultado',
                  data: message.chart.values,
                  backgroundColor: 'rgba(106, 192, 171, 0.5)',
                  borderColor: 'rgba(85, 165, 146, 0.9)',
                  borderWidth: 1,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: {
                legend: { display: false },
              },
              scales: {
                y: {
                  beginAtZero: true,
                },
              },
            },
          });
        });

        const table = createChartTable(message.chart);
        if (table) {
          bubble.appendChild(table);
        }
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
