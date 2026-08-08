import { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import { WorkerMetapageClient } from "../client.ts";

export const uploadFileTool: Tool = {
  name: "upload_file",
  description: "Upload a file to the storage system and get a key that can be used as input to jobs.",
  inputSchema: {
    type: "object",
    properties: {
      filename: {
        type: "string",
        description: "The filename for the uploaded file",
      },
      content: {
        type: "string",
        description: "The file content (text content will be uploaded as-is, for binary data use base64 encoding)",
      },
      contentType: {
        type: "string",
        description: "MIME type of the content",
        default: "text/plain",
      },
      encoding: {
        type: "string",
        description: "Content encoding: 'text' for plain text, 'base64' for binary data",
        enum: ["text", "base64"],
        default: "text",
      },
    },
    required: ["filename", "content"],
  },
};

export async function handleUploadFile(
  request: CallToolRequest,
  client: WorkerMetapageClient,
): Promise<CallToolResult> {
  try {
    const args = request.params.arguments as any;
    const { filename, content, contentType = "text/plain", encoding = "text" } = args;

    // Generate a unique key for the file
    const timestamp = Date.now();
    const randomSuffix = Math.random().toString(36).substring(2, 8);
    const key = `${timestamp}-${randomSuffix}-${filename}`;

    // Convert content based on encoding
    let uploadContent: string | ArrayBuffer;
    if (encoding === "base64") {
      // Decode base64 to binary
      const binaryString = atob(content);
      const bytes = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      uploadContent = bytes.buffer;
    } else {
      uploadContent = content;
    }

    const uploadedKey = await client.uploadFile(key, uploadContent);

    const response = {
      success: true,
      filename,
      key: uploadedKey,
      contentType,
      encoding,
      size: encoding === "base64" ? Math.ceil(content.length * 3 / 4) : content.length,
      downloadUrl: `${client.baseUrl}/f/${uploadedKey}`,
      message: `File '${filename}' uploaded successfully with key: ${uploadedKey}`,
    };

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  } catch (error: unknown) {
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            {
              success: false,
              error: (error as Error).message,
              message: `Failed to upload file: ${request.params.arguments?.filename}`,
            },
            null,
            2,
          ),
        },
      ],
      isError: true,
    };
  }
}
