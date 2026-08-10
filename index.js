require('dotenv').config()
const express = require('express')
const cors = require('cors')
const axios = require('axios')

const app = express()
const PORT = process.env.PORT || 3000

const CHAT_MODEL = process.env.QWEN_MODEL || 'qwen3.7-plus'
const CHAT_API_URL =
  process.env.QWEN_API_URL ||
  process.env.DASHSCOPE_BASE_URL ||
  'https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation'

const IMAGE_MODEL = process.env.QWEN_IMAGE_MODEL || 'qwen-image-2.0'
const IMAGE_API_URL =
  process.env.WAN_IMAGE_API_URL ||
  process.env.DASHSCOPE_BASE_URL ||
  'https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation'

function getApiKey() {
  return process.env.QWEN_API_KEY || process.env.DASHSCOPE_API_KEY || ''
}

function redactKey(key) {
  if (!key) return '(missing)'
  if (key.startsWith('sk-sp-')) return 'sk-sp-***'
  if (key.startsWith('sk-ws-')) return 'sk-ws-***'
  if (key.startsWith('sk-')) return 'sk-***'
  return 'unknown-format'
}

function sendSse(res, data) {
  const text = String(data)
  const payload = text
    .split(/\r?\n/)
    .map((line) => `data: ${line}`)
    .join('\n')

  res.write(`${payload}\n\n`)
}

function parseSseFrames(raw, onPayload) {
  let buffer = raw
  let separatorIndex = buffer.indexOf('\n\n')

  while (separatorIndex !== -1) {
    const frame = buffer.slice(0, separatorIndex)
    buffer = buffer.slice(separatorIndex + 2)

    const dataLines = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith('data:'))

    for (const line of dataLines) {
      const payload = line.replace(/^data:\s*/, '').trim()
      if (payload) {
        onPayload(payload)
      }
    }

    separatorIndex = buffer.indexOf('\n\n')
  }

  return buffer
}

function extractTextContent(message) {
  if (!message) return ''

  const { content } = message

  if (typeof content === 'string') {
    return content
  }

  if (Array.isArray(content)) {
    return content.map((item) => item?.text || item?.content || '').join('')
  }

  if (content && typeof content === 'object' && typeof content.text === 'string') {
    return content.text
  }

  return ''
}

function buildChatContent(prompt, image) {
  const text = prompt ? String(prompt).trim() : ''

  if (!image) {
    return text
  }

  return [
    {
      image_url: {
        url: image
      }
    },
    {
      text: text || '请描述这张图片。'
    }
  ]
}

function buildImageContent(prompt, image) {
  const text = prompt ? String(prompt).trim() : ''
  return [
    {
      image: image
    },
    {
      text: text || '请基于输入图片进行合理修改，并尽量保留原图主体。'
    }
  ]
}

function extractImageUrls(content) {
  if (Array.isArray(content)) {
    return content
      .map((item) => item?.image || item?.image_url || item?.imageUrl)
      .filter(Boolean)
  }
  if (typeof content === 'string' && content) {
    return [content]
  }
  return []
}

app.use(cors())
app.use(express.json({ limit: '50mb' }))

app.get('/', (req, res) => {
  res.json({
    name: 'AI Chat Server',
    status: 'ok',
    service: 'Qwen AI API'
  })
})
app.all('/api/stream-chat', async (req, res) => {
  const prompt = req.method === 'GET' ? req.query.prompt : req.body?.prompt
  const image = req.method === 'GET' ? req.query.image : req.body?.image

  if (!prompt && !image) {
    return res.status(400).send('missing prompt or image')
  }

  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8')
  res.setHeader('Cache-Control', 'no-cache, no-transform')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('X-Accel-Buffering', 'no')
  res.flushHeaders()

  const apiKey = getApiKey()
  if (!apiKey) {
    sendSse(res, 'backend missing QWEN_API_KEY or DASHSCOPE_API_KEY')
    sendSse(res, '[DONE]')
    return res.end()
  }

  const controller = new AbortController()

  try {
    const result = await axios.post(
      CHAT_API_URL,
      {
        model: CHAT_MODEL,
        stream: true,
        input: {
          messages: [
            {
              role: 'user',
              content: buildChatContent(prompt, image)
            }
          ]
        },
        parameters: {
          result_format: 'message',
          incremental_output: true
        }
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
          Accept: 'text/event-stream',
          'X-DashScope-SSE': 'enable'
        },
        responseType: 'stream',
        timeout: 60000,
        signal: controller.signal
      }
    )

    let buffer = ''

    result.data.on('data', (chunkBuffer) => {
      buffer += chunkBuffer.toString('utf8')

      buffer = parseSseFrames(buffer, (payload) => {
        if (payload === '[DONE]') {
          sendSse(res, '[DONE]')
          controller.abort()
          res.end()
          return
        }

        try {
          const jsonObj = JSON.parse(payload)
          const message = jsonObj.output?.choices?.[0]?.message
          const text = extractTextContent(message)
          if (text) {
            sendSse(res, text)
          }
        } catch (err) {
          sendSse(res, payload)
        }
      })
    })

    result.data.on('end', () => {
      if (!res.writableEnded) {
        sendSse(res, '[DONE]')
        res.end()
      }
    })

    result.data.on('error', (streamErr) => {
      if (!res.writableEnded) {
        sendSse(res, `stream error: ${streamErr.message}`)
        sendSse(res, '[DONE]')
        res.end()
      }
    })

    req.on('close', () => {
      controller.abort()
      if (!res.writableEnded) {
        res.end()
      }
    })
  } catch (err) {
    console.error('Chat API error:', err.response?.data || err.message)
    const errMsg = err.response?.data?.message || err.message
    sendSse(res, `call failed: ${errMsg}`)
    sendSse(res, '[DONE]')
    res.end()
  }
})

app.post('/api/image-edit', async (req, res) => {
  const { prompt, image } = req.body || {}

  if (!image) {
    return res.status(400).json({ message: 'missing image' })
  }

  const apiKey = getApiKey()
  if (!apiKey) {
    return res.status(400).json({ message: 'backend missing QWEN_API_KEY or DASHSCOPE_API_KEY' })
  }

  try {
    const result = await axios.post(
      IMAGE_API_URL,
      {
        model: IMAGE_MODEL,
        input: {
          messages: [
            {
              role: 'user',
              content: buildImageContent(prompt, image)
            }
          ]
        },
        parameters: {
          n: 1,
          negative_prompt: '',
          prompt_extend: false,
          watermark: false,
          size: '2048*2048'
        }
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
          Accept: 'application/json'
        },
        timeout: 120000
      }
    )

    const content = result.data?.output?.choices?.[0]?.message?.content
    const images = extractImageUrls(content)

    if (!images.length) {
      console.error('Image API unexpected response:', result.data)
      return res.status(500).json({
        message: 'image api returned no image',
        raw: result.data
      })
    }

    res.json({
      text: result.data?.output?.message || '图片已生成',
      images,
      raw: result.data
    })
  } catch (err) {
    console.error('Image edit error:', err.response?.data || err.message)
    const errMsg = err.response?.data?.message || err.message
    res.status(500).json({ message: errMsg })
  }
})

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Qwen stream server started on port ${PORT}`)
  console.log(`Chat model: ${CHAT_MODEL}`)
  console.log(`Image model: ${IMAGE_MODEL}`)
  console.log(`Image API: ${IMAGE_API_URL}`)
  console.log(
  "QWEN KEY:",
  process.env.QWEN_API_KEY ? "存在" : "不存在"
)
})