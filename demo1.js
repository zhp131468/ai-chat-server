require('dotenv').config()
const express = require('express')
const cors = require('cors')
const axios = require('axios')
const app = express()
const PORT = process.env.PORT || 3000

const CHAT_MODEL = process.env.QWEN_MODEL || 'qwen4-max'
const CHAT_API_URL =
  process.env.QWEN_API_URL ||
  process.env.DASHSCOPE_BASE_URL ||
  'https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation'
const MODELSCOPE_BASE_URL = 'https://api-inference.modelscope.cn';
const MODELSCOPE_TOKEN = process.env.MODELSCOPE_TOKEN;
const IMAGE_MODEL = process.env.QWEN_IMAGE_MODEL || 'Qwen/Qwen-Image-Edit'
const IMAGE_API_URL =
  process.env.WAN_IMAGE_API_URL ||
  process.env.DASHSCOPE_BASE_URL ||
  'https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation'

function getApiKey () {
  return process.env.QWEN_API_KEY || process.env.DASHSCOPE_API_KEY || ''
}

function redactKey (key) {
  if (!key) return '(missing)'
  if (key.startsWith('sk-sp-')) return 'sk-sp-***'
  if (key.startsWith('sk-ws-')) return 'sk-ws-***'
  if (key.startsWith('sk-')) return 'sk-***'
  return 'unknown-format'
}

function sendSse (res, data) {
  const text = String(data)
  const payload = text
    .split(/\r?\n/)
    .map((line) => `data: ${line}`)
    .join('\n')

  res.write(`${payload}\n\n`)
}

function parseSseFrames (raw, onPayload) {
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

function extractTextContent (message) {
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

function buildChatContent (prompt, image) {
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

function buildImageContent (prompt, image) {
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

function extractImageUrls (content) {
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
// 创建图片编辑任务

app.post('/api/image-edit', async (req, res) => {
  const startTime = Date.now();

  try {
    if (!MODELSCOPE_TOKEN) {
      return res.status(500).json({
        success: false,
        message: '未配置 MODELSCOPE_TOKEN'
      });
    }

    const { prompt, image } = req.body || {};
    console.log('[图片编辑请求]', {
      contentType: req.headers['content-type'],
      hasPrompt: !!prompt,
      hasImage: !!image,
      imageType: typeof image,
      imagePrefix: typeof image === 'string' ? image.slice(0, 30) : ''
    });
    if (!image || typeof image !== 'string') {
      return res.status(400).json({
        success: false,
        message: '请上传原始图片'
      });
    }

    if (!prompt || !prompt.trim()) {
      return res.status(400).json({
        success: false,
        message: '请输入图片编辑指令'
      });
    }

    // 检查图片格式
    const match = image.match(
      /^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/
    );

    if (!match) {
      return res.status(400).json({
        success: false,
        message: '图片格式不正确，仅支持 JPG、PNG、WEBP'
      });
    }

    const imageBuffer = Buffer.from(match[2], 'base64');

    if (!imageBuffer.length || imageBuffer.length > 10 * 1024 * 1024) {
      return res.status(400).json({
        success: false,
        message: '图片不能为空或超过 10MB'
      });
    }

    // 提交 ModelScope 图片编辑任务
    const response = await axios.post(
      `${MODELSCOPE_BASE_URL}/v1/images/generations`,
      {
        model: IMAGE_MODEL,
        prompt: prompt.trim(),
        image_url: image
      },
      {
        headers: {
          Authorization: `Bearer ${MODELSCOPE_TOKEN}`,
          'Content-Type': 'application/json',
          'X-ModelScope-Async-Mode': 'true'
        },
        timeout: 60000
      }
    );

    const taskId = response.data?.task_id;

    if (!taskId) {
      console.error('ModelScope 返回异常:', response.data);

      return res.status(502).json({
        success: false,
        message: '魔搭没有返回有效的任务 ID'
      });
    }

    return res.json({
      success: true,
      taskId,
      message: '图片编辑任务已提交',
      duration: `${((Date.now() - startTime) / 1000).toFixed(1)}s`
    });

  } catch (error) {
    console.error(
      'ModelScope 图片编辑提交失败:',
      error.response?.data || error.message
    );

    return res.status(502).json({
      success: false,
      message:
        error.response?.data?.message ||
        error.message ||
        '图片编辑任务提交失败'
    });
  }
});


app.get('/api/image-edit/:taskId', async (req, res) => {
  const { taskId } = req.params;

  try {
    const response = await axios.get(
      `${MODELSCOPE_BASE_URL}/v1/tasks/${encodeURIComponent(taskId)}`,
      {
        headers: {
          Authorization: `Bearer ${MODELSCOPE_TOKEN}`,
          'X-ModelScope-Task-Type': 'image_generation'
        },
        timeout: 30000
      }
    );

    // 1. 打印 ModelScope 原始返回数据
    console.log(
      '[ModelScope 原始任务状态]',
      JSON.stringify(response.data, null, 2)
    );

    const data = response.data;

    // 2. 检查任务状态
    console.log('[图片任务状态]', {
      taskId,
      taskStatus: data?.task_status,
      message: data?.message
    });

    // 3. 任务成功
    if (data.task_status === 'SUCCEED') {
      const imageUrl = data.output_images?.[0];

      if (!imageUrl) {
        console.error('[图片任务异常] 任务成功，但没有返回图片地址');

        return res.status(502).json({
          success: false,
          status: 'failed',
          message: '图片生成成功，但没有获取到图片地址'
        });
      }

      return res.json({
        success: true,
        status: 'succeeded',
        imageUrl
      });
    }

    // 4. 任务失败
    if (data.task_status === 'FAILED') {
      return res.json({
        success: false,
        status: 'failed',
        message: data.message || '图片生成失败'
      });
    }

    // 5. 其他状态：记录真实状态，暂时返回处理中
    console.log('[图片任务仍在处理中]', data.task_status);

    return res.json({
      success: true,
      status: 'processing',
      taskStatus: data.task_status || 'UNKNOWN'
    });

  } catch (error) {
    // 6. 打印完整异常信息
    console.error('[图片任务轮询异常]', {
      message: error.message,
      code: error.code,
      status: error.response?.status,
      data: error.response?.data
    });

    return res.status(502).json({
      success: false,
      status: 'error',
      message: '查询图片编辑状态失败',
      detail: error.response?.data || error.message,
      code: error.code
    });
  }
});

// app.post('/api/image-edit', async (req, res) => {
//   const startTime = Date.now()

//   const { prompt, image } = req.body || {}

//   if (!image) {
//     return res.status(400).json({
//       message: 'missing image'
//     })
//   }


//   const apiKey = getApiKey()

//   if (!apiKey) {
//     return res.status(400).json({
//       message:
//         'backend missing QWEN_API_KEY or DASHSCOPE_API_KEY'
//     })
//   }


//   try {

//     const result = await axios.post(
//       IMAGE_API_URL,
//       {
//         model: IMAGE_MODEL,

//         input: {
//           messages: [
//             {
//               role: 'user',
//               content: buildImageContent(prompt, image)
//             }
//           ]
//         },

//         parameters: {
//           n: 1,
//           negative_prompt: '',
//           prompt_extend: false,
//           watermark: false,
//           size: '2048*2048'
//         }
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${apiKey}`,
//           'Content-Type': 'application/json',
//           Accept: 'application/json'
//         },

//         timeout: 120000
//       }
//     )


//     const content = result.data?.output?.choices?.[0]?.message?.content


//     const images = extractImageUrls(content)


//     if (!images.length) {

//       console.error(
//         'Image API unexpected response:',
//         result.data
//       )


//       return res.status(500).json({

//         message: 'image api returned no image',

//         raw: result.data

//       })
//     }



//     // ===== 新增耗时计算 =====

//     const duration = ((Date.now() - startTime) / 1000).toFixed(1)
//     res.json({

//       success: true,

//       text:
//         result.data?.output?.message ||
//         '图片已生成',


//       images,


//       // 新增字段
//       duration: `${duration}s`,


//       // 方便调试
//       timestamp: Date.now()

//     })


//   } catch (err) {


//     console.error(
//       'Image edit error:',
//       err.response?.data ||
//       err.message
//     )


//     const errMsg =
//       err.response?.data?.message ||
//       err.message


//     res.status(500).json({

//       success: false,

//       message: errMsg

//     })

//   }
// })
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Qwen stream server started on port ${PORT}`)
  console.log(`Chat model: ${CHAT_MODEL}`)
  console.log(`Image model: ${IMAGE_MODEL}`)
  console.log(`Image API: ${IMAGE_API_URL}`)
  console.log("QWEN KEY:", process.env.QWEN_API_KEY ? "存在" : "不存在")
  console.log('开始生成图片:', new Date().toISOString())
  console.log('MODELSCOPE_BASE_URL地址:', MODELSCOPE_BASE_URL)
})


// 这个方案是集成调用魔塔的Qwen/Qwen-Image-Edit